import argparse
import sys

from .main import run_job


def main():
    parser = argparse.ArgumentParser(description="YouTube Search Metadata Downloader")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not write files or notify"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    exit_code = run_job(args.config, args.dry_run, args.verbose)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
