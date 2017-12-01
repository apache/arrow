declare module "json-bignum" {
    function stringify(obj: any): string;
    function parse(str: string): any;
    class BigNumber {
        numberStr: string;
        constructor (numberStr: string);
    }
}
