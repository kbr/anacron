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


def format_item_list(data, separator="\n", divider=":", column_width=None):
    """
    Helperfunction: takes a string representing a list of data-pairs
    separated by a given separator, with the data in the data-pair
    separated by the given divider (i.e. "1:2\n3:4" etc). Formats the
    data pairs according to the longest first item, so that the dividers
    (the colons in the exaple data given above) are vertical aligned.
    Returns the new formatted string.
    """
    keys = []
    values = []
    for line in data.split(separator):
        key, value = line.split(divider, 1)
        keys.append(key)
        values.append(value)
    if column_width is None:
        # here is pylint wrong:
        # pylint: disable=unnecessary-lambda
        column_width = len(max(keys, key=lambda x: len(x)))
    template = f"{{:{column_width}}}{divider} {{}}"
    return separator.join(template.format(k, v) for k, v in zip(keys, values))


def report_info():
    """Report the settings values."""
    settings = interface.get_settings()
    settings = format_item_list(str(settings))
    output = f"\nSettings\n{settings}\n"
    print(output)


def report_tasks_on_due():
    """Report all tasks waiting for execution and are on due."""
    tasks = interface.get_tasks_on_due()
    print(f"\ntasks found: {len(tasks)}")
    divider = "-" * 50
    for task in tasks:
        task = format_item_list(str(task))
        print(f"{divider}\n{task}")
    print(divider)
    print()


def reset_defaults():
    """Reset the settings with the default values."""
    settings = interface.get_settings()
    settings.max_workers = MAX_WORKERS_DEFAULT
    settings.running_workers = 0
    interface.set_settings(settings)
    print("\nReset settings default values:")
    report_info()


def set_max_workers(workers):
    """Change value for the number of allowed max_workers."""
    settings = interface.get_settings()
    settings.max_workers = workers
    interface.set_settings(settings)
    print(f"Set max_workers to {workers}")


def delete_database():
    """
    Delete the database and create the database again with the default
    settings.
    """
    answer = input("Sure to delete the current database? [y/n]: ")
    if answer.lower() == 'y':
        interface.db_name.unlink()
        # this could be all, because at next time anacron gets imported
        # a new database will get created.
        # However, we do this here and now:
        # pylint: disable=protected-access
        interface._init_database()
    else:
        print("abort command")


def get_command_line_arguments():
    """Get the command line arguments."""
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
    parser.add_argument(
        "-d", "--get-tasks-on-due",
        dest="get_tasks_on_due",
        action="store_true",
        help="lists all tasks waiting for execution and are on due."
    )
    return parser.parse_args()


def main(args=None):
    """entry point."""
    if not args:
        args = get_command_line_arguments()
    if args.info:
        report_info()
    elif args.reset_defaults:
        reset_defaults()
    elif args.max_workers:
        set_max_workers(args.max_workers)
    elif args.get_tasks_on_due:
        report_tasks_on_due()
    elif args.delete_database:
        delete_database()


if __name__ == "__main__":
    main(get_command_line_arguments())
