from .schema import migrate
from .engine import Engine
import logging
import argparse


def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    argparser = argparse.ArgumentParser(prog="sqlorm.migrate")
    argparser.add_argument("--from", type=int, dest="from_version")
    argparser.add_argument("--to", type=int, dest="to_version")
    argparser.add_argument("--dryrun", action="store_true")
    argparser.add_argument("--ignore-schema-version", action="store_true")
    argparser.add_argument("--path", default=".")
    argparser.add_argument("engine_uri")

    args = argparser.parse_args(argv)

    logging.info(f"Connecting to {args.engine_uri}")
    with Engine.from_uri(args.engine_uri):
        migrate(args.path,
                from_version=args.from_version,
                to_version=args.to_version,
                use_schema_version=not args.ignore_schema_version,
                dryrun=args.dryrun,
                logger=logging)


if __name__ == "__main__":
    main()