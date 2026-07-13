"""CLI entry point for dlia-etl."""

import argparse
import logging
import sys

from dlia_etl.db import get_engine
from dlia_etl.registry import list_tasks, run_task, run_all


def main():
    parser = argparse.ArgumentParser(
        prog="dlia-etl",
        description="Detroit Land Information Archive ETL pipeline",
    )
    subparsers = parser.add_subparsers(dest="command")
    
    # List
    subparsers.add_parser("list", help="List all registered tasks")
    
    # Run
    run_parser = subparsers.add_parser("run", help="Run ETL tasks")
    run_group = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--task", help="Run a single task by name")
    run_group.add_argument("--phase", type=int, help="Run all tasks in a phase")
    run_group.add_argument("--all", action="store_true", help="Run all tasks")

    run_parser.add_argument(
        "--source-db",
        default="ipds",
        help="Source database name (default: ipds)",
    )
    run_parser.add_argument(
        "--target-db",
        default="ipds",
        help="Target database name (default: ipds)",
    )

    run_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Show INFO-level log messages",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if getattr(args, "verbose", False) else logging.WARNING,
        format="%(message)s",
    )

    # Import tasks to trigger registration
    import dlia_etl.tasks  # noqa: F401

    if args.command == "list":
        tasks = list_tasks()
        if not tasks:
            print("No tasks registered.")
            return

        print(f"{'Name':<30} {'Phase':<8} {'Description'}")
        print("-" * 70)
        for t in tasks:
            print(f"{t.name:<30} {t.phase:<8} {t.description}")
        return

    if args.command == "run":
        source = get_engine(args.source_db)
        target = get_engine(args.target_db)

        if args.task:
            print(f"Running task: {args.task}")
            result = run_task(args.task, source, target)
            _print_result(result)
            sys.exit(0 if result.success else 1)

        elif args.phase is not None:
            print(f"Running all phase {args.phase} tasks")
            results = run_all(source, target, phase=args.phase)
            _print_results(results)
            sys.exit(0 if all(r.success for r in results) else 1)

        elif args.all:
            print("Running all tasks")
            results = run_all(source, target)
            _print_results(results)
            sys.exit(0 if all(r.success for r in results) else 1)

    else:
        parser.print_help()


def _print_result(result):
    status = "OK" if result.success else "FAILED"
    print(f"  [{status}] {result.task_name}: {result.rows_inserted} rows")
    if result.error:
        print(f"    Error: {result.error}")


def _print_results(results):
    for r in results:
        _print_result(r)
    succeeded = sum(1 for r in results if r.success)
    print(f"\n{succeeded}/{len(results)} tasks succeeded.")
