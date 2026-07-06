"""Task registry and runner infrastructure.

Each ETL task is a function decorated with @task("name", phase=N).
Tasks self-register at import time. The CLI uses this registry to
discover and run tasks.
"""

from dataclasses import dataclass
from typing import Callable

from sqlalchemy import Engine


@dataclass
class TaskResult:
    task_name: str
    rows_inserted: int
    success: bool
    error: str | None = None


TaskFunc = Callable[[Engine, Engine], TaskResult]


@dataclass
class TaskInfo:
    name: str
    phase: int
    func: TaskFunc
    description: str = ""


_REGISTRY: dict[str, TaskInfo] = {}


def task(name: str, phase: int = 1, description: str = ""):
    """Decorator to register an ETL task.

    Usage:
        @task("rental_vericast", phase=2, description="Rental and Vericast parcel data")
        def run(source: Engine, target: Engine) -> TaskResult:
            ...
    """

    def decorator(func: TaskFunc) -> TaskFunc:
        _REGISTRY[name] = TaskInfo(
            name=name,
            phase=phase,
            func=func,
            description=description,
        )
        return func

    return decorator


def get_task(name: str) -> TaskInfo:
    """Get a registered task by name. Raises KeyError if not found."""
    return _REGISTRY[name]


def list_tasks() -> list[TaskInfo]:
    """List all registered tasks, sorted by phase then name."""
    return sorted(_REGISTRY.values(), key=lambda t: (t.phase, t.name))


def run_task(name: str, source: Engine, target: Engine) -> TaskResult:
    """Run a single task by name."""
    info = get_task(name)
    try:
        return info.func(source, target)
    except Exception as e:
        return TaskResult(
            task_name=name,
            rows_inserted=0,
            success=False,
            error=str(e),
        )


def run_all(
    source: Engine,
    target: Engine,
    phase: int | None = None,
) -> list[TaskResult]:
    """Run all registered tasks, optionally filtered by phase."""
    results = []
    for info in list_tasks():
        if phase is not None and info.phase != phase:
            continue
        results.append(run_task(info.name, source, target))
    return results
