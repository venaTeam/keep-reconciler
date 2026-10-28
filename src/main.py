import uvicorn

from src.app import app
from src.config import settings

if __name__ == "__main__":
    # log_config=None: uvicorn's default LOGGING_CONFIG would run dictConfig here,
    # which flushes-and-closes handlers configured by setup_logging() (incl. the
    # FluentBitHandler background sender). setup_logging() already wired the
    # uvicorn.* loggers via CONFIG, so there is nothing left for uvicorn to apply.
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_config=None)
