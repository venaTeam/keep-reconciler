import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.lifecycle import lifespan
from src.logging import setup_logging
from src.state import ConsumerState

setup_logging()
logger = logging.getLogger("keep-reconciler")

app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    state: ConsumerState | None = getattr(app.state, "consumer_state", None)
    if state is None:
        return {"status": "starting"}
    counters = state.get_counters()
    if not state.is_healthy():
        return JSONResponse(
            status_code=503, content={"status": "degraded", "counters": counters}
        )
    # Backoff means Keep is unreachable, not that this pod is broken: stay 200
    # so a liveness probe doesn't restart it.
    if state.is_in_backoff():
        return {"status": "backoff", "counters": counters}
    return {"status": "ok", "counters": counters}
