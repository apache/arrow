//! Apples-to-apples comparison of FSST vs OnPair16 (the paper's 16-byte-max-token
//! variant, arXiv:2508.02280) at 12- and 16-bit dictionary sizes, on the same
//! string corpora.
//!
//! Both codecs run in a single Rust process so encode/decode throughput is
//! measured under one harness. Pin to a single core with `taskset -c 0`.
//!
//! Corpora:
//!   * TPC-H string columns (o_comment, p_name, l_comment, c_comment),
//!     generated in-process via tpchgen at scale factor 1.
//!   * ClickBench: real `hits.parquet`-style data if ONPAIR_BENCH_PARQUET is
//!     set (+ optional ONPAIR_BENCH_COLUMN), else a synthetic URL corpus.
//!
//! Size accounting (raw codec output, no downstream integer compression):
//!   * OnPair  = dict bytes + dict offsets(u32) + codes(u16) + row offsets(u32)
//!   * FSST    = symbol table + symbol lengths + code bytes + row offsets(u32)
//!
//! Both count an (n+1) u32 row-offset vector so the comparison is fair.

use std::hint::black_box;
use std::mem::MaybeUninit;
use std::time::Instant;

use arrow_array::cast::AsArray;
use fsst::Compressor;
use onpair::{Config, MaxDictBits, Threshold, compress as onpair_compress};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, OrderGenerator, PartGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, OrderArrow, PartArrow, RecordBatchIterator, SupplierArrow,
};

const BATCH_SIZE: usize = 8192 * 8;
/// Every corpus is truncated/generated to exactly this many rows for a fair
/// equal-N comparison.
const TARGET_ROWS: usize = 500_000;
const ENCODE_ITERS: usize = 3;
const DECODE_ITERS: usize = 10;

/// A packed string corpus: concatenated bytes + (n+1) u64 offsets.
struct Corpus {
    name: String,
    bytes: Vec<u8>,
    offsets: Vec<u64>,
    /// Per-row slices, precomputed for FSST training/compression.
    n_rows: usize,
}

impl Corpus {
    fn new(name: impl Into<String>, bytes: Vec<u8>, offsets: Vec<u64>) -> Self {
        let n_rows = offsets.len() - 1;
        Corpus { name: name.into(), bytes, offsets, n_rows }
    }
    fn raw_bytes(&self) -> usize {
        self.bytes.len()
    }
    fn rows(&self) -> Vec<&[u8]> {
        (0..self.n_rows)
            .map(|i| &self.bytes[self.offsets[i] as usize..self.offsets[i + 1] as usize])
            .collect()
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ───────────────────────────── OnPair ─────────────────────────────

struct Measured {
    label: String,
    compressed_bytes: usize,
    encode_mibs: f64,
    decode_mibs: f64,
}

fn run_onpair(c: &Corpus, bits: u8, threshold: f64) -> Measured {
    let cfg = Config {
        max_dict_bits: MaxDictBits::new(bits).unwrap(),
        threshold: Threshold::new(threshold).unwrap(),
        seed: Some(42),
    };
    // Compressed size + a live column for decode timing.
    let col = onpair_compress(&c.bytes, &c.offsets, cfg).unwrap();
    let compressed = col.dict.bytes().len()
        + col.dict.offsets().len() * 4
        + col.codes.len() * 2
        + col.row_offsets.len() * 4;

    // Encode throughput: full train + compress, median of ENCODE_ITERS.
    let mut enc = Vec::with_capacity(ENCODE_ITERS);
    for _ in 0..ENCODE_ITERS {
        let t = Instant::now();
        let out = onpair_compress(black_box(&c.bytes), black_box(&c.offsets), cfg).unwrap();
        let dt = t.elapsed().as_secs_f64();
        black_box(&out);
        enc.push(mib(c.raw_bytes()) / dt);
    }

    // Decode throughput: whole-column decompress_into.
    let cap = col.view().decoded_len() + 16; // + tail padding
    // Correctness: whole-column decode must reconstruct the concatenated input.
    {
        let mut buf: Vec<MaybeUninit<u8>> = vec![MaybeUninit::uninit(); cap];
        let n = unsafe { col.view().decompress_into(&mut buf) };
        let decoded: &[u8] = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, n) };
        assert_eq!(n, c.raw_bytes(), "OnPair{bits} decoded len mismatch on {}", c.name);
        assert!(decoded == c.bytes.as_slice(), "OnPair{bits} roundtrip mismatch on {}", c.name);
    }
    let mut dec = Vec::with_capacity(DECODE_ITERS);
    for _ in 0..DECODE_ITERS {
        let mut buf: Vec<MaybeUninit<u8>> = vec![MaybeUninit::uninit(); cap];
        let t = Instant::now();
        // SAFETY: buf sized to decoded_len()+padding; view from a trusted column.
        let n = unsafe { col.view().decompress_into(&mut buf) };
        let dt = t.elapsed().as_secs_f64();
        black_box(&buf[..n]);
        dec.push(mib(c.raw_bytes()) / dt);
    }

    Measured {
        label: format!("OnPair{bits}"),
        compressed_bytes: compressed,
        encode_mibs: median(enc),
        decode_mibs: median(dec),
    }
}

// ───────────────────────────── FSST ─────────────────────────────

/// Train + compress every row into one concatenated code buffer with
/// (n+1) u32 offsets. Returns (compressor, codes, offsets).
fn fsst_encode(rows: &Vec<&[u8]>) -> (Compressor, Vec<u8>, Vec<u32>) {
    let compressor = Compressor::train(rows);
    let total: usize = rows.iter().map(|r| r.len()).sum();
    let mut codes: Vec<u8> = Vec::with_capacity(2 * total + 8 * rows.len() + 16);
    let mut offsets: Vec<u32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    let mut scratch: Vec<u8> = Vec::with_capacity(1024);
    for r in rows {
        scratch.clear();
        // FSST worst case: 2 bytes per input byte + a few for escapes.
        let need = 2 * r.len() + 16;
        if scratch.capacity() < need {
            scratch.reserve(need - scratch.capacity());
        }
        // SAFETY: scratch has capacity for the FSST worst-case output of `r`.
        unsafe { compressor.compress_into(r, &mut scratch) };
        codes.extend_from_slice(&scratch);
        offsets.push(codes.len() as u32);
    }
    (compressor, codes, offsets)
}

fn run_fsst(c: &Corpus) -> Measured {
    let rows = c.rows();

    let (compressor, codes, offsets) = fsst_encode(&rows);
    let compressed = std::mem::size_of_val(compressor.symbol_table())
        + std::mem::size_of_val(compressor.symbol_lengths())
        + codes.len()
        + offsets.len() * 4;

    // Encode throughput.
    let mut enc = Vec::with_capacity(ENCODE_ITERS);
    for _ in 0..ENCODE_ITERS {
        let t = Instant::now();
        let out = fsst_encode(black_box(&rows));
        let dt = t.elapsed().as_secs_f64();
        black_box(&out);
        enc.push(mib(c.raw_bytes()) / dt);
    }

    // Decode throughput: decompress the whole concatenated code stream at once
    // (FSST codes are context-free, so this reconstructs concatenated plaintext).
    let decompressor = compressor.decompressor();
    let cap = c.raw_bytes() + 16;
    // Correctness: decoding the concatenated code stream reconstructs the input.
    {
        let mut buf: Vec<MaybeUninit<u8>> = vec![MaybeUninit::uninit(); cap];
        let n = decompressor.decompress_into(&codes, &mut buf);
        let decoded: &[u8] = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, n) };
        assert_eq!(n, c.raw_bytes(), "FSST decoded len mismatch on {}", c.name);
        assert!(decoded == c.bytes.as_slice(), "FSST roundtrip mismatch on {}", c.name);
    }
    let mut dec = Vec::with_capacity(DECODE_ITERS);
    for _ in 0..DECODE_ITERS {
        let mut buf: Vec<MaybeUninit<u8>> = vec![MaybeUninit::uninit(); cap];
        let t = Instant::now();
        let n = decompressor.decompress_into(black_box(&codes), &mut buf);
        let dt = t.elapsed().as_secs_f64();
        black_box(&buf[..n]);
        dec.push(mib(c.raw_bytes()) / dt);
    }

    Measured {
        label: "FSST".to_string(),
        compressed_bytes: compressed,
        encode_mibs: median(enc),
        decode_mibs: median(dec),
    }
}

// ───────────────────────────── Corpora ─────────────────────────────

/// Load any TPC-H string column, dispatching to its table generator by the
/// column-name prefix (o_/l_/c_/p_/s_).
fn tpch_column(col: &str) -> Corpus {
    // Scale factor per table so it yields >= TARGET_ROWS rows (rows/SF at SF1:
    // lineitem 6.0M, orders 1.5M, customer 150k, part 200k, supplier 10k), then
    // truncate to exactly TARGET_ROWS.
    let sf: f64 = match col.split('_').next().unwrap() {
        "l" | "o" => 1.0,
        "c" => 4.0,   // 150k * 4 = 600k
        "p" => 3.0,   // 200k * 3 = 600k
        "s" => 50.0,  // 10k * 50 = 500k
        _ => 1.0,
    };
    let idx_of = |schema: &arrow_schema::Schema| {
        schema.fields().iter().position(|f| f.name() == col).unwrap_or_else(|| {
            panic!("column {col} not found in table schema")
        })
    };
    let (bytes, offsets) = match col.split('_').next().unwrap() {
        "l" => {
            let it = LineItemArrow::new(LineItemGenerator::new(sf, 1, 1)).with_batch_size(BATCH_SIZE);
            let schema = it.schema().clone();
            collect(it, idx_of(&schema))
        }
        "o" => {
            let it = OrderArrow::new(OrderGenerator::new(sf, 1, 1)).with_batch_size(BATCH_SIZE);
            let schema = it.schema().clone();
            collect(it, idx_of(&schema))
        }
        "c" => {
            let it = CustomerArrow::new(CustomerGenerator::new(sf, 1, 1)).with_batch_size(BATCH_SIZE);
            let schema = it.schema().clone();
            collect(it, idx_of(&schema))
        }
        "p" => {
            let it = PartArrow::new(PartGenerator::new(sf, 1, 1)).with_batch_size(BATCH_SIZE);
            let schema = it.schema().clone();
            collect(it, idx_of(&schema))
        }
        "s" => {
            let it = SupplierArrow::new(SupplierGenerator::new(sf, 1, 1)).with_batch_size(BATCH_SIZE);
            let schema = it.schema().clone();
            collect(it, idx_of(&schema))
        }
        other => panic!("unknown table prefix {other} for column {col}"),
    };
    Corpus::new(format!("tpch/{col}"), bytes, offsets)
}

fn collect<I>(batches: I, idx: usize) -> (Vec<u8>, Vec<u64>)
where
    I: Iterator<Item = arrow_array::RecordBatch>,
{
    let mut bytes = Vec::new();
    let mut offsets: Vec<u64> = vec![0];
    'outer: for batch in batches {
        let arr = batch.column(idx).as_string_view();
        for v in arr.iter() {
            let s = v.unwrap_or("").as_bytes();
            bytes.extend_from_slice(s);
            offsets.push(bytes.len() as u64);
            if offsets.len() > TARGET_ROWS {
                break 'outer;
            }
        }
    }
    (bytes, offsets)
}

fn clickbench_corpus() -> Corpus {
    if let Ok(path) = std::env::var("ONPAIR_BENCH_PARQUET") {
        if let Some((bytes, offsets, colname)) = read_parquet(&path) {
            return Corpus::new(format!("clickbench/{colname}"), bytes, offsets);
        }
        eprintln!("warning: could not read {path}, falling back to synthetic");
    }
    let (bytes, offsets) = synthetic_clickbench_urls(TARGET_ROWS);
    Corpus::new("clickbench/synthetic-urls", bytes, offsets)
}

fn read_parquet(_path: &str) -> Option<(Vec<u8>, Vec<u64>, String)> {
    // Only wired when ONPAIR_BENCH_PARQUET is set; requires the `parquet` crate.
    // Left unimplemented to keep the default build light; synthetic is used.
    None
}

fn synthetic_clickbench_urls(n: usize) -> (Vec<u8>, Vec<u64>) {
    const HOSTS: &[&str] = &[
        "https://www.yandex.ru", "https://www.google.com", "https://news.ycombinator.com",
        "https://www.example.com", "https://docs.example.org", "https://api.example.net",
        "http://m.yandex.ru", "https://maps.example.com", "https://shop.example.com",
        "ftp://files.example.com",
    ];
    const PATHS: &[&str] = &[
        "/", "/page", "/news", "/search?q=", "/profile", "/login", "/api/v1/data",
        "/static/asset.png", "/blog/post-", "/feed.xml", "/sitemap.xml", "/users/",
        "/admin/dashboard", "/categories/electronics", "/cart/checkout",
    ];
    const TAILS: &[&str] = &["", "alpha", "beta", "gamma", "delta", "001", "002", "003"];
    let mut bytes = Vec::new();
    let mut offsets: Vec<u64> = vec![0];
    let mut x = 0x9E3779B97F4A7C15u64;
    for _ in 0..n {
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        let h = HOSTS[(x as usize) % HOSTS.len()];
        let p = PATHS[((x >> 16) as usize) % PATHS.len()];
        let t = TAILS[((x >> 32) as usize) % TAILS.len()];
        let num = (x >> 48) as u16;
        let s = format!("{h}{p}{t}{num}");
        bytes.extend_from_slice(s.as_bytes());
        offsets.push(bytes.len() as u64);
    }
    (bytes, offsets)
}

/// Synthetic JSON "event" objects: a fixed schema (repeated keys + structural
/// punctuation) with per-row varying values. High-cardinality overall (unique
/// ids/timestamps), but saturated with shared <=16-byte fragments — the shape a
/// Variant/JSON column takes. Newline-free per row.
fn synthetic_variant_json(n: usize) -> (Vec<u8>, Vec<u64>) {
    const EVENTS: &[&str] = &["click", "view", "purchase", "signup", "logout", "search"];
    const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "JP", "IN", "BR", "CA"];
    const TIERS: &[&str] = &["free", "pro", "enterprise"];
    let mut bytes = Vec::new();
    let mut offsets: Vec<u64> = vec![0];
    let mut x = 0x1234_5678_9ABC_DEF0u64;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for _ in 0..n {
        let uid = next() % 10_000_000;
        let ev = EVENTS[(next() as usize) % EVENTS.len()];
        let country = COUNTRIES[(next() as usize) % COUNTRIES.len()];
        let tier = TIERS[(next() as usize) % TIERS.len()];
        let hh = next() % 24;
        let mm = next() % 60;
        let ss = next() % 60;
        let day = 1 + next() % 28;
        let amount = (next() % 100000) as f64 / 100.0;
        let session = next();
        let s = format!(
            "{{\"user_id\":{uid},\"event\":\"{ev}\",\"ts\":\"2024-03-{day:02}T{hh:02}:{mm:02}:{ss:02}Z\",\
\"country\":\"{country}\",\"tier\":\"{tier}\",\"amount\":{amount:.2},\"session\":\"{session:016x}\"}}"
        );
        bytes.extend_from_slice(s.as_bytes());
        offsets.push(bytes.len() as u64);
    }
    (bytes, offsets)
}

// ───────────────────────────── main ─────────────────────────────

/// Write each corpus as a newline-delimited .txt (one row per line) into `dir`,
/// so the C++ harness reads byte-identical inputs. All these columns are
/// newline-free (TPC-H comments/names, synthetic URLs), so line-delimiting is
/// lossless here.
fn dump_corpora(dir: &str) {
    use std::io::Write;
    std::fs::create_dir_all(dir).expect("create dump dir");
    // A spread across data shapes: free text, multi-word names/types,
    // high-cardinality addresses, patterned IDs, and low-cardinality enums.
    let tpch_cols = [
        // free text
        "o_comment", "l_comment", "c_comment", "p_comment", "s_comment",
        // multi-word names / types
        "p_name", "p_type", "c_name", "s_name",
        // high-cardinality addresses
        "c_address", "s_address",
        // patterned IDs / numbers
        "o_clerk", "c_phone",
        // low-cardinality enums / small vocab
        "o_orderpriority", "l_shipmode", "p_brand", "p_container", "c_mktsegment",
    ];
    let mut corpora: Vec<Corpus> = tpch_cols.iter().map(|c| tpch_column(c)).collect();
    corpora.push(clickbench_corpus());
    {
        let (bytes, offsets) = synthetic_variant_json(TARGET_ROWS);
        corpora.push(Corpus::new("variant/json-events", bytes, offsets));
    }
    for c in &corpora {
        let fname = c.name.replace('/', "_");
        let path = format!("{dir}/{fname}.txt");
        let f = std::fs::File::create(&path).expect("create file");
        let mut w = std::io::BufWriter::new(f);
        for i in 0..c.n_rows {
            let s = &c.bytes[c.offsets[i] as usize..c.offsets[i + 1] as usize];
            assert!(!s.contains(&b'\n'), "row contains newline in {}", c.name);
            w.write_all(s).unwrap();
            w.write_all(b"\n").unwrap();
        }
        w.flush().unwrap();
        eprintln!("[dump] {path}: {} rows, {:.2} MiB", c.n_rows, mib(c.raw_bytes()));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--dump-corpora") {
        let dir = args.get(pos + 1).map(String::as_str).unwrap_or("corpora");
        dump_corpora(dir);
        return;
    }

    // TPC-H uses threshold 0.2 (matches onpair's tpch bench); ClickBench 0.5.
    let corpora: Vec<(Corpus, f64)> = vec![
        (tpch_column("o_comment"), 0.2),
        (tpch_column("l_comment"), 0.2),
        (tpch_column("c_comment"), 0.2),
        (tpch_column("p_name"), 0.2),
        (clickbench_corpus(), 0.5),
    ];

    println!(
        "{:<26} {:>10} {:>10}  {:>9} {:>9} {:>9}  {:>9} {:>9} {:>9}",
        "corpus", "rows", "raw MiB",
        "ratio", "enc MiB/s", "dec MiB/s", "", "", ""
    );
    println!("{}", "─".repeat(120));

    for (c, threshold) in &corpora {
        let fsst = run_fsst(c);
        let op12 = run_onpair(c, 12, *threshold);
        let op16 = run_onpair(c, 16, *threshold);

        println!(
            "{:<26} {:>10} {:>10.2}",
            c.name, c.n_rows, mib(c.raw_bytes())
        );
        for m in [&fsst, &op12, &op16] {
            let ratio = c.raw_bytes() as f64 / m.compressed_bytes as f64;
            println!(
                "  {:<24} {:>10} {:>10.2}  {:>8.3}x {:>9.1} {:>9.1}",
                m.label, "", mib(m.compressed_bytes),
                ratio, m.encode_mibs, m.decode_mibs
            );
        }
        // Head-to-head deltas: OnPair16 vs FSST.
        let ratio_fsst = c.raw_bytes() as f64 / fsst.compressed_bytes as f64;
        let ratio_op16 = c.raw_bytes() as f64 / op16.compressed_bytes as f64;
        println!(
            "  → OnPair16 vs FSST: ratio {:+.1}%, encode {:+.1}%, decode {:+.1}%",
            (ratio_op16 / ratio_fsst - 1.0) * 100.0,
            (op16.encode_mibs / fsst.encode_mibs - 1.0) * 100.0,
            (op16.decode_mibs / fsst.decode_mibs - 1.0) * 100.0,
        );
        println!();
    }
}
