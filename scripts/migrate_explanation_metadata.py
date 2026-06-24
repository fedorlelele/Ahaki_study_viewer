import argparse

try:
    from scripts.explanation_metadata import migrate_database
except ModuleNotFoundError:
    from explanation_metadata import migrate_database


def parse_args():
    parser = argparse.ArgumentParser(
        description="Add model_name/review_status metadata columns to explanations."
    )
    parser.add_argument(
        "--db",
        default="output/ahaki.sqlite",
        help="Path to SQLite database.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    migrate_database(args.db)
    print(f"Migrated explanation metadata columns in {args.db}")


if __name__ == "__main__":
    main()
