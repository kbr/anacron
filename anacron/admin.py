"""
admin.py

Administration tool to access the database.
"""

import argparse

from .sql_interface import (
    interface,
    MAX_WORKERS_DEFAULT,
)


PGM_NAME = "anacron command line tool"
PGM_DESCRIPTION = """
    Allows access to the anacron database to change default
    settings and to inspect waiting tasks and stored results.
"""

def report_info():
    """Report the settings values."""
    settings = interface.get_settings()
    output = f"\nSettings{settings}\n"
    print(output)


def reset_defaults():
    """Reset the settings with the default values."""
    settings = interface.get_settings()
    settings.max_workers = MAX_WORKERS_DEFAULT
    settings.running_workers = 0
    interface.set_settings(settings)
    print(f"\nReset settings default values:")
    report_info()


def set_max_workers(workers):
    """Change value for the number of allowed max_workers."""
    settings = interface.get_settings()
    settings.max_workers = workers
    interface.set_settings(settings)
    print(f"Set max_workers to {workers}")


def delete_database():
    answer = input("Sure to delete the current database? [y/n]: ")
    if answer.lower() == 'y':
        interface.db_name.unlink()
        # this could be all, because at next time anacron gets imported
        # a new database will get created.
        # However, we do this here and now:
        interface._init_database()
    else:
        print("abort command")


def get_command_line_arguments():
    parser = argparse.ArgumentParser(
        prog=PGM_NAME,
        description=PGM_DESCRIPTION,
    )
    parser.add_argument(
        "-i", "--info",
        action="store_true",
        help="provide information about the settings, number of waiting tasks "\
             "and result entries."
    )
    parser.add_argument(
        "--reset-defaults",
        dest="reset_defaults",
        action="store_true",
        help="restore the default settings: max_workers=1, running_workers=0."
    )
    parser.add_argument(
        "--delete-database",
        dest="delete_database",
        action="store_true",
        help="delete the current database and creates a new clean one with "\
             "the default settings."
    )
    parser.add_argument(
        "--set-max-workers",
        dest="max_workers",
        type=int,
        help="set number of maximum worker processes."
    )
    return parser.parse_args()


def main(args=None):
    if not args:
        args = get_command_line_arguments()
    if args.info:
        report_info()
    elif args.reset_defaults:
        reset_defaults()
    elif args.max_workers:
        set_max_workers(args.max_workers)
    elif args.delete_database:
        delete_database()


if __name__ == "__main__":
    main(get_command_line_arguments())
