export class IndexAccessProxyHandler<T extends object = any> implements ProxyHandler<T> {
    get(target: any, key: string, receiver: any) {
        if (typeof key === "string") { // Need to check because key can be a symbol, such as [Symbol.iterator].
            const idx = +key; // Convert the key to a number
            if (idx === idx) { // Basically an inverse NaN check
                return (receiver || target).get(idx);
            }
        }

        return Reflect.get(target, key, receiver);
    }

    set(target: any, key: string, value: any, receiver: any) {
        if (typeof key === "string") { // Need to check because key can be a symbol, such as [Symbol.iterator].
            const idx = +key; // Convert the key to a number
            if (idx === idx) { // Basically an inverse NaN check
                (receiver || target).set(idx, value);
                return true; // Is this correct?
            }
        }

        return Reflect.set(target, key, value, receiver);
    }
}
