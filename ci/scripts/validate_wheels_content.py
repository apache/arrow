import argparse
import re
import zipfile


def validate_wheel(wheel):
    f = zipfile.ZipFile(wheel)
    outliers = [
        info.filename for info in f.filelist if not re.match(
            r'(pyarrow/|pyarrow-[-.\w\d]+\.dist-info/)', info.filename
        )
    ]
    assert not outliers, f"Unexpected contents in wheel: {sorted(outliers)}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", type=str, required=True,
                        help="wheel filename to validate")
    args = parser.parse_args()
    validate_wheel(args.wheel)


if __name__ == '__main__':
    main()
