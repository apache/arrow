import { Struct } from '../types';

export function toString<T>(source: Struct<T>, options?: any) {
    const index = typeof options === 'object' ? options && !!options.index
                : typeof options === 'boolean' ? !!options
                : false;
    const { length } = source;
    if (length <= 0) { return ''; }
    const rows = new Array(length + 1);
    const maxColumnWidths = [] as number[];
    rows[0] = source.columns.map((_, i) => source.key(i));
    index && rows[0].unshift('Index');
    for (let i = -1, n = rows.length - 1; ++i < n;) {
        rows[i + 1] = [...source.get(i)!];
        index && rows[i + 1].unshift(i);
    }
    // Pass one to convert to strings and count max column widths
    for (let i = -1, n = rows.length; ++i < n;) {
        const row = rows[i];
        for (let j = -1, k = row.length; ++j < k;) {
            const val = row[j] = `${row[j]}`;
            maxColumnWidths[j] = !maxColumnWidths[j]
                ? val.length
                : Math.max(maxColumnWidths[j], val.length);
        }
    }
    // Pass two to pad each one to max column width
    for (let i = -1, n = rows.length; ++i < n;) {
        const row = rows[i];
        for (let j = -1, k = row.length; ++j < k;) {
            row[j] = leftPad(row[j], ' ', maxColumnWidths[j]);
        }
        rows[i] = row.join(', ');
    }
    return rows.join('\n');
}

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}
