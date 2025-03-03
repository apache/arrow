import hashlib
import sys


def gearhash(n: int, seed: int):
    value = bytes([seed] * 64 + [n] * 64)
    hasher = hashlib.md5(value)
    return hasher.hexdigest()[:16]


def print_table(seed: int, length=256, comma=True):
    table = [gearhash(n, seed=seed) for n in range(length)]
    print(f"{{ // seed = {seed}")
    for i in range(0, length, 4):
        print("  ", end="")
        values = [f"0x{value}" for value in table[i:i + 4]]
        values = ", ".join(values)
        print(f"  {values}", end=",\n" if i < length - 4 else "\n")
    print(" }", end=", " if comma else "")


if __name__ == "__main__":
    print("{")
    n = int(sys.argv[1])
    for seed in range(n):
        print_table(seed, comma=seed < n)
    print("}")