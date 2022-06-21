import argparse

from dotenv import load_dotenv


def lambda_handler(event, context) -> None:
    main(dry_run=False)


def main(dry_run: bool) -> None:
    if dry_run:
        print('(DRY RUN)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    parser.add_argument('--no-dry-run', action='store_true')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main(dry_run=(not args.no_dry_run))
