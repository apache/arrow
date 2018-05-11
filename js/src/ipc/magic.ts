export const PADDING = 4;
export const MAGIC_STR = 'ARROW1';
export const MAGIC = new Uint8Array(MAGIC_STR.length);

for (let i = 0; i < MAGIC_STR.length; i += 1 | 0) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

export function checkForMagicArrowString(buffer: Uint8Array, index = 0) {
    for (let i = -1, n = MAGIC.length; ++i < n;) {
        if (MAGIC[i] !== buffer[index + i]) {
            return false;
        }
    }
    return true;
}

export const magicLength = MAGIC.length;
export const magicAndPadding = magicLength + PADDING;
export const magicX2AndPadding = magicLength * 2 + PADDING;
