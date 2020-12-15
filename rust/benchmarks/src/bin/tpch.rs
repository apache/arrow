// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Benchmark derived from TPC-H. This is not an official TPC-H benchmark.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, DateUnit, Field, Schema};
use arrow::util::pretty;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::{CsvFile, MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::csv::CsvExec;
use datafusion::prelude::*;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct BenchmarkOpt {
    /// Query number
    #[structopt(short, long)]
    query: usize,

    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of threads to use for parallel execution
    #[structopt(short = "c", long = "concurrency", default_value = "2")]
    concurrency: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "4096")]
    batch_size: usize,

    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// File format: `csv` or `parquet`
    #[structopt(short = "f", long = "format", default_value = "csv")]
    file_format: String,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,
}

#[derive(Debug, StructOpt)]
struct ConvertOpt {
    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Output file format: `csv` or `parquet`
    #[structopt(short = "f", long = "format")]
    file_format: String,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "TPC-H", about = "TPC-H Benchmarks.")]
enum TpchOpt {
    Benchmark(BenchmarkOpt),
    Convert(ConvertOpt),
}

const TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

#[tokio::main]
async fn main() -> Result<()> {
    match TpchOpt::from_args() {
        TpchOpt::Benchmark(opt) => benchmark(opt).await,
        TpchOpt::Convert(opt) => convert_tbl(opt).await,
    }
}

async fn benchmark(opt: BenchmarkOpt) -> Result<()> {
    println!("Running benchmarks with the following options: {:?}", opt);
    let config = ExecutionConfig::new()
        .with_concurrency(opt.concurrency)
        .with_batch_size(opt.batch_size);
    let mut ctx = ExecutionContext::with_config(config);

    // register tables
    for table in TABLES {
        let table_provider =
            get_table(opt.path.to_str().unwrap(), table, opt.file_format.as_str())?;
        if opt.mem_table {
            println!("Loading table '{}' into memory", table);
            let start = Instant::now();

            let memtable =
                MemTable::load(table_provider.as_ref(), opt.batch_size).await?;
            println!(
                "Loaded table '{}' into memory in {} ms",
                table,
                start.elapsed().as_millis()
            );
            ctx.register_table(table, Box::new(memtable));
        } else {
            ctx.register_table(table, table_provider);
        }
    }

    // run benchmark
    for i in 0..opt.iterations {
        let start = Instant::now();
        let plan = create_logical_plan(&mut ctx, opt.query)?;
        execute_query(&mut ctx, &plan, opt.debug).await?;
        println!(
            "Query {} iteration {} took {} ms",
            opt.query,
            i,
            start.elapsed().as_millis()
        );
    }

    Ok(())
}

fn create_logical_plan(ctx: &mut ExecutionContext, query: usize) -> Result<LogicalPlan> {
    match query {

        // original
        // 1 => ctx.create_logical_plan(
        //     "select
        //         l_returnflag,
        //         l_linestatus,
        //         sum(l_quantity) as sum_qty,
        //         sum(l_extendedprice) as sum_base_price,
        //         sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        //         sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        //         avg(l_quantity) as avg_qty,
        //         avg(l_extendedprice) as avg_price,
        //         avg(l_discount) as avg_disc,
        //         count(*) as count_order
        //     from
        //         lineitem
        //     where
        //         l_shipdate <= date '1998-12-01' - interval '90' day (3)
        //     group by
        //         l_returnflag,
        //         l_linestatus
        //     order by
        //         l_returnflag,
        //         l_linestatus;"
        // ),
        1 => ctx.create_logical_plan(
            "select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            from
                lineitem
            where
                l_shipdate <= date '1998-09-02'
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus;",
        ),

        2 => ctx.create_logical_plan(
            "select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            from
                part,
                supplier,
                partsupp,
                nation,
                region
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and p_size = 15
                and p_type like '%BRASS'
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'EUROPE'
                and ps_supplycost = (
                    select
                        min(ps_supplycost)
                    from
                        partsupp,
                        supplier,
                        nation,
                        region
                    where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
                )
            order by
                s_acctbal desc,
                n_name,
                s_name,
                p_partkey;"
        ),

        3 => ctx.create_logical_plan(
            "select
                l_orderkey,
                sum(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            from
                customer,
                orders,
                lineitem
            where
                c_mktsegment = 'BUILDING'
                and c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate < date '1995-03-15'
                and l_shipdate > date '1995-03-15'
            group by
                l_orderkey,
                o_orderdate,
                o_shippriority
            order by
                revenue desc,
                o_orderdate;"
        ),

        4 => ctx.create_logical_plan(
            "select
                o_orderpriority,
                count(*) as order_count
            from
                orders
            where
                o_orderdate >= '1993-07-01'
                and o_orderdate < date '1993-07-01' + interval '3' month
                and exists (
                    select
                        *
                    from
                        lineitem
                    where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
                )
            group by
                o_orderpriority
            order by
                o_orderpriority;"
        ),

        // original
        // 5 => ctx.create_logical_plan(
        //     "select
        //         n_name,
        //         sum(l_extendedprice * (1 - l_discount)) as revenue
        //     from
        //         customer,
        //         orders,
        //         lineitem,
        //         supplier,
        //         nation,
        //         region
        //     where
        //         c_custkey = o_custkey
        //         and l_orderkey = o_orderkey
        //         and l_suppkey = s_suppkey
        //         and c_nationkey = s_nationkey
        //         and s_nationkey = n_nationkey
        //         and n_regionkey = r_regionkey
        //         and r_name = 'ASIA'
        //         and o_orderdate >= date '1994-01-01'
        //         and o_orderdate < date '1994-01-01' + interval '1' year
        //     group by
        //         n_name
        //     order by
        //         revenue desc;"
        // ),
        5 => ctx.create_logical_plan(
            "select
                n_name,
                sum(l_extendedprice * (1 - l_discount)) as revenue
            from
                customer,
                orders,
                lineitem,
                supplier,
                nation,
                region
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and l_suppkey = s_suppkey
                and c_nationkey = s_nationkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'ASIA'
                and o_orderdate >= date '1994-01-01'
                and o_orderdate < date '1995-01-01'
            group by
                n_name
            order by
                revenue desc;"
        ),

        // original
        // 6 => ctx.create_logical_plan(
        //     "select
        //         sum(l_extendedprice * l_discount) as revenue
        //     from
        //         lineitem
        //     where
        //         l_shipdate >= date '1994-01-01'
        //         and l_shipdate < date '1994-01-01' + interval '1' year
        //         and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        //         and l_quantity < 24;"
        // ),
        6 => ctx.create_logical_plan(
            "select
                sum(l_extendedprice * l_discount) as revenue
            from
                lineitem
            where
                l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1995-01-01'
                and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                and l_quantity < 24;"
        ),

        7 => ctx.create_logical_plan(
            "select
                supp_nation,
                cust_nation,
                l_year,
                sum(volume) as revenue
            from
                (
                    select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                    from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                    where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
                ) as shipping
            group by
                supp_nation,
                cust_nation,
                l_year
            order by
                supp_nation,
                cust_nation,
                l_year;"
        ),

        8 => ctx.create_logical_plan(
            "select
                o_year,
                sum(case
                    when nation = 'BRAZIL' then volume
                    else 0
                end) / sum(volume) as mkt_share
            from
                (
                    select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                    from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                    where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'AMERICA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
                ) as all_nations
            group by
                o_year
            order by
                o_year;"
        ),

        9 => ctx.create_logical_plan(
            "select
                nation,
                o_year,
                sum(amount) as sum_profit
            from
                (
                    select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                    from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                    where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%green%'
                ) as profit
            group by
                nation,
                o_year
            order by
                nation,
                o_year desc;"
        ),

        // 10 => ctx.create_logical_plan(
        //     "select
        //         c_custkey,
        //         c_name,
        //         sum(l_extendedprice * (1 - l_discount)) as revenue,
        //         c_acctbal,
        //         n_name,
        //         c_address,
        //         c_phone,
        //         c_comment
        //     from
        //         customer,
        //         orders,
        //         lineitem,
        //         nation
        //     where
        //         c_custkey = o_custkey
        //         and l_orderkey = o_orderkey
        //         and o_orderdate >= date '1993-10-01'
        //         and o_orderdate < date '1993-10-01' + interval '3' month
        //         and l_returnflag = 'R'
        //         and c_nationkey = n_nationkey
        //     group by
        //         c_custkey,
        //         c_name,
        //         c_acctbal,
        //         c_phone,
        //         n_name,
        //         c_address,
        //         c_comment
        //     order by
        //         revenue desc;"
        // ),
        10 => ctx.create_logical_plan(
            "select
                c_custkey,
                c_name,
                sum(l_extendedprice * (1 - l_discount)) as revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            from
                customer,
                orders,
                lineitem,
                nation
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate >= date '1993-10-01'
                and o_orderdate < date '1994-01-01'
                and l_returnflag = 'R'
                and c_nationkey = n_nationkey
            group by
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
            order by
                revenue desc;"
        ),

        11 => ctx.create_logical_plan(
            "select
                ps_partkey,
                sum(ps_supplycost * ps_availqty) as value
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
            group by
                ps_partkey having
                    sum(ps_supplycost * ps_availqty) > (
                        select
                            sum(ps_supplycost * ps_availqty) * 0.0001
                        from
                            partsupp,
                            supplier,
                            nation
                        where
                            ps_suppkey = s_suppkey
                            and s_nationkey = n_nationkey
                            and n_name = 'GERMANY'
                    )
            order by
                value desc;"
        ),

        // original
        // 12 => ctx.create_logical_plan(
        //     "select
        //         l_shipmode,
        //         sum(case
        //             when o_orderpriority = '1-URGENT'
        //                 or o_orderpriority = '2-HIGH'
        //                 then 1
        //             else 0
        //         end) as high_line_count,
        //         sum(case
        //             when o_orderpriority <> '1-URGENT'
        //                 and o_orderpriority <> '2-HIGH'
        //                 then 1
        //             else 0
        //         end) as low_line_count
        //     from
        //         orders,
        //         lineitem
        //     where
        //         o_orderkey = l_orderkey
        //         and l_shipmode in ('MAIL', 'SHIP')
        //         and l_commitdate < l_receiptdate
        //         and l_shipdate < l_commitdate
        //         and l_receiptdate >= date '1994-01-01'
        //         and l_receiptdate < date '1994-01-01' + interval '1' year
        //     group by
        //         l_shipmode
        //     order by
        //         l_shipmode;"
        // ),
        12 => ctx.create_logical_plan(
            "select
                l_shipmode,
                sum(case
                    when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                    else 0
                end) as high_line_count,
                sum(case
                    when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                    else 0
                end) as low_line_count
            from
                lineitem
            join
                orders
            on
                l_orderkey = o_orderkey
            where
                (l_shipmode = 'MAIL' or l_shipmode = 'SHIP')
                and l_commitdate < l_receiptdate
                and l_shipdate < l_commitdate
                and l_receiptdate >= date '1994-01-01'
                and l_receiptdate < date '1995-01-01'
            group by
                l_shipmode
            order by
                l_shipmode;"
        ),

        13 => ctx.create_logical_plan(
            "select
                c_count,
                count(*) as custdist
            from
                (
                    select
                        c_custkey,
                        count(o_orderkey)
                    from
                        customer left outer join orders on
                            c_custkey = o_custkey
                            and o_comment not like '%special%requests%'
                    group by
                        c_custkey
                ) as c_orders (c_custkey, c_count)
            group by
                c_count
            order by
                custdist desc,
                c_count desc;"
        ),

        14 => ctx.create_logical_plan(
            "select
                100.00 * sum(case
                    when p_type like 'PROMO%'
                        then l_extendedprice * (1 - l_discount)
                    else 0
                end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
            from
                lineitem,
                part
            where
                l_partkey = p_partkey
                and l_shipdate >= date '1995-09-01'
                and l_shipdate < date '1995-10-01';"
        ),

        15 => ctx.create_logical_plan(
            "create view revenue0 (supplier_no, total_revenue) as
                select
                    l_suppkey,
                    sum(l_extendedprice * (1 - l_discount))
                from
                    lineitem
                where
                    l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-01-01' + interval '3' month
                group by
                    l_suppkey;

            select
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            from
                supplier,
                revenue0
            where
                s_suppkey = supplier_no
                and total_revenue = (
                    select
                        max(total_revenue)
                    from
                        revenue0
                )
            order by
                s_suppkey;

            drop view revenue0;"
        ),

        16 => ctx.create_logical_plan(
            "select
                p_brand,
                p_type,
                p_size,
                count(distinct ps_suppkey) as supplier_cnt
            from
                partsupp,
                part
            where
                p_partkey = ps_partkey
                and p_brand <> 'Brand#45'
                and p_type not like 'MEDIUM POLISHED%'
                and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                and ps_suppkey not in (
                    select
                        s_suppkey
                    from
                        supplier
                    where
                        s_comment like '%Customer%Complaints%'
                )
            group by
                p_brand,
                p_type,
                p_size
            order by
                supplier_cnt desc,
                p_brand,
                p_type,
                p_size;"
        ),

        17 => ctx.create_logical_plan(
            "select
                sum(l_extendedprice) / 7.0 as avg_yearly
            from
                lineitem,
                part
            where
                p_partkey = l_partkey
                and p_brand = 'Brand#23'
                and p_container = 'MED BOX'
                and l_quantity < (
                    select
                        0.2 * avg(l_quantity)
                    from
                        lineitem
                    where
                        l_partkey = p_partkey
                );"
        ),

        18 => ctx.create_logical_plan(
            "select
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice,
                sum(l_quantity)
            from
                customer,
                orders,
                lineitem
            where
                o_orderkey in (
                    select
                        l_orderkey
                    from
                        lineitem
                    group by
                        l_orderkey having
                            sum(l_quantity) > 300
                )
                and c_custkey = o_custkey
                and o_orderkey = l_orderkey
            group by
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice
            order by
                o_totalprice desc,
                o_orderdate;"
        ),

        19 => ctx.create_logical_plan(
            "select
                sum(l_extendedprice* (1 - l_discount)) as revenue
            from
                lineitem,
                part
            where
                (
                    p_partkey = l_partkey
                    and p_brand = 'Brand#12'
                    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    and l_quantity >= 1 and l_quantity <= 1 + 10
                    and p_size between 1 and 5
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
                or
                (
                    p_partkey = l_partkey
                    and p_brand = 'Brand#23'
                    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    and l_quantity >= 10 and l_quantity <= 10 + 10
                    and p_size between 1 and 10
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
                or
                (
                    p_partkey = l_partkey
                    and p_brand = 'Brand#34'
                    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    and l_quantity >= 20 and l_quantity <= 20 + 10
                    and p_size between 1 and 15
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                );"
        ),

        20 => ctx.create_logical_plan(
            "select
                s_name,
                s_address
            from
                supplier,
                nation
            where
                s_suppkey in (
                    select
                        ps_suppkey
                    from
                        partsupp
                    where
                        ps_partkey in (
                            select
                                p_partkey
                            from
                                part
                            where
                                p_name like 'forest%'
                        )
                        and ps_availqty > (
                            select
                                0.5 * sum(l_quantity)
                            from
                                lineitem
                            where
                                l_partkey = ps_partkey
                                and l_suppkey = ps_suppkey
                                and l_shipdate >= date '1994-01-01'
                                and l_shipdate < 'date 1994-01-01' + interval '1' year
                        )
                )
                and s_nationkey = n_nationkey
                and n_name = 'CANADA'
            order by
                s_name;"
        ),

        21 => ctx.create_logical_plan(
            "select
                s_name,
                count(*) as numwait
            from
                supplier,
                lineitem l1,
                orders,
                nation
            where
                s_suppkey = l1.l_suppkey
                and o_orderkey = l1.l_orderkey
                and o_orderstatus = 'F'
                and l1.l_receiptdate > l1.l_commitdate
                and exists (
                    select
                        *
                    from
                        lineitem l2
                    where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
                )
                and not exists (
                    select
                        *
                    from
                        lineitem l3
                    where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
                )
                and s_nationkey = n_nationkey
                and n_name = 'SAUDI ARABIA'
            group by
                s_name
            order by
                numwait desc,
                s_name;"
        ),

        22 => ctx.create_logical_plan(
            "select
                cntrycode,
                count(*) as numcust,
                sum(c_acctbal) as totacctbal
            from
                (
                    select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_acctbal
                    from
                        customer
                    where
                        substring(c_phone from 1 for 2) in
                            ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                            select
                                avg(c_acctbal)
                            from
                                customer
                            where
                                c_acctbal > 0.00
                                and substring(c_phone from 1 for 2) in
                                    ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                            select
                                *
                            from
                                orders
                            where
                                o_custkey = c_custkey
                        )
                ) as custsale
            group by
                cntrycode
            order by
                cntrycode;"
        ),

        _ => unimplemented!("invalid query. Expected value between 1 and 22"),
    }
}

async fn execute_query(
    ctx: &mut ExecutionContext,
    plan: &LogicalPlan,
    debug: bool,
) -> Result<()> {
    if debug {
        println!("Logical plan:\n{:?}", plan);
    }
    let plan = ctx.optimize(&plan)?;
    if debug {
        println!("Optimized logical plan:\n{:?}", plan);
    }
    let physical_plan = ctx.create_physical_plan(&plan)?;
    let result = collect(physical_plan).await?;
    if debug {
        pretty::print_batches(&result)?;
    }
    Ok(())
}

async fn convert_tbl(opt: ConvertOpt) -> Result<()> {
    for table in TABLES {
        let schema = get_schema(table);

        let path = format!("{}/{}.tbl", opt.input_path.to_str().unwrap(), table);
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .file_extension(".tbl");

        let ctx = ExecutionContext::new();
        let csv = Arc::new(CsvExec::try_new(&path, options, None, 4096)?);
        let output_path = opt.output_path.to_str().unwrap().to_owned();

        match opt.file_format.as_str() {
            "csv" => ctx.write_csv(csv, output_path).await?,
            "parquet" => ctx.write_parquet(csv, output_path).await?,
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Invalid output format: {}",
                    other
                )))
            }
        }
    }

    Ok(())
}

fn get_table(
    path: &str,
    table: &str,
    table_format: &str,
) -> Result<Box<dyn TableProvider + Send + Sync>> {
    match table_format {
        // dbgen creates .tbl ('|' delimited) files
        "tbl" => {
            let path = format!("{}/{}.tbl", path, table);
            let schema = get_schema(table);
            let options = CsvReadOptions::new()
                .schema(&schema)
                .delimiter(b'|')
                .file_extension(".tbl");

            Ok(Box::new(CsvFile::try_new(&path, options)?))
        }
        "csv" => {
            let path = format!("{}/{}", path, table);
            let schema = get_schema(table);
            let options = CsvReadOptions::new().schema(&schema).has_header(true);

            Ok(Box::new(CsvFile::try_new(&path, options)?))
        }
        "parquet" => {
            let path = format!("{}/{}", path, table);
            Ok(Box::new(ParquetTable::try_new(&path)?))
        }
        other => {
            unimplemented!("Invalid file format '{}'", other);
        }
    }
}

fn get_schema(table: &str) -> Schema {
    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::UInt32, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::UInt32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Float64, false), // decimal
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::UInt32, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::UInt32, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Float64, false), // decimal
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::UInt32, false),
            Field::new("ps_suppkey", DataType::UInt32, false),
            Field::new("ps_availqty", DataType::UInt32, false),
            Field::new("ps_supplycost", DataType::Float64, false), // decimal
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::UInt32, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::UInt32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false), // decimal
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::UInt32, false),
            Field::new("o_custkey", DataType::UInt32, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false), // decimal
            Field::new("o_orderdate", DataType::Date32(DateUnit::Day), false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::UInt32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::UInt32, false),
            Field::new("l_partkey", DataType::UInt32, false),
            Field::new("l_suppkey", DataType::UInt32, false),
            Field::new("l_linenumber", DataType::UInt32, false),
            Field::new("l_quantity", DataType::Float64, false), // decimal
            Field::new("l_extendedprice", DataType::Float64, false), // decimal
            Field::new("l_discount", DataType::Float64, false), // decimal
            Field::new("l_tax", DataType::Float64, false),      // decimal
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32(DateUnit::Day), false),
            Field::new("l_commitdate", DataType::Date32(DateUnit::Day), false),
            Field::new("l_receiptdate", DataType::Date32(DateUnit::Day), false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::UInt32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::UInt32, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::UInt32, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}
