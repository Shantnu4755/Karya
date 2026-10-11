from collections.abc import Callable

from ..exceptions import NotFoundError

TaskFn = Callable[[dict], None]


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskFn] = {}
        self._descriptions: dict[str, str] = {}

    def register(self, name: str, description: str = "") -> Callable[[TaskFn], TaskFn]:
        def decorator(fn: TaskFn) -> TaskFn:
            if name in self._tasks:
                raise ValueError(f"Task already registered: {name}")
            self._tasks[name] = fn
            self._descriptions[name] = description
            return fn

        return decorator

    def get(self, name: str) -> TaskFn:
        try:
            return self._tasks[name]
        except KeyError as exc:
            raise NotFoundError(f"No handler for task '{name}'") from exc

    def describe(self) -> dict[str, str]:
        """Task name -> description, sorted by name."""
        return dict(sorted(self._descriptions.items()))


registry = TaskRegistry()
