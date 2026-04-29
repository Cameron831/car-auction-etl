import argparse
import sys

from app.sources.bat import cli as bat_cli
from app.sources.carsandbids import cli as carsandbids_cli


def build_parser():
    parser = argparse.ArgumentParser(description="Run car auction ETL commands.")
    parser.add_argument("source", choices=("bat", "cab"))
    return parser


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv

    if argv and argv[0] == "bat":
        return bat_cli.main(argv[1:])
    if argv and argv[0] == "cab":
        return carsandbids_cli.main(argv[1:])

    args, remaining_args = build_parser().parse_known_args(argv)

    if args.source == "bat":
        return bat_cli.main(remaining_args)
    if args.source == "cab":
        return carsandbids_cli.main(remaining_args)

    raise AssertionError(f"Unhandled source: {args.source}")


if __name__ == "__main__":
    main()
