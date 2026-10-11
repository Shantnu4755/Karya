"""Worker process: python -m app.worker (same image as the API, different command)."""

from ..config import Config
from ..extensions import db
from ..logging_setup import setup_logging
from . import registry
from .worker import Worker


def main() -> None:
    config = Config.from_env()
    setup_logging(config.log_level)
    db.init_app(config.database_url)
    Worker(queue=config.worker_queue, registry=registry).run()


if __name__ == "__main__":
    main()
