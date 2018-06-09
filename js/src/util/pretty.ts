
export function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

export function valueToString(x: any) {
    return typeof x === 'string' ? `"${x}"` : ArrayBuffer.isView(x) ? `[${x}]` : JSON.stringify(x);
}
