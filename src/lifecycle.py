import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import settings
from src.kafka_consumer import consume
from src.state import ConsumerState

logger = logging.getLogger("keep-reconciler")


def _start_consumer_threads(
    shutdown_event: threading.Event, consumer_state: ConsumerState
):
    def run_consumer():
        restart_times = []
        while not shutdown_event.is_set():
            now = time.time()
            restart_times = [
                t for t in restart_times if now - t < settings.keep_consumer_restart_window
            ]
            if len(restart_times) >= settings.keep_consumer_restart_limit:
                logger.error(
                    "Consumer restarted %d times in %d seconds; exiting process",
                    settings.keep_consumer_restart_limit,
                    settings.keep_consumer_restart_window,
                )
                os._exit(1)
            restart_times.append(now)

            logger.info("Starting consumer thread")
            try:
                consume(shutdown_event, consumer_state)
            except Exception:
                logger.exception("Consumer thread died unexpectedly")
                consumer_state.mark_unhealthy()
            if shutdown_event.is_set():
                break
            logger.info("Consumer thread exited; restarting in 5 seconds")
            time.sleep(5)

    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()

    def watch_consumer():
        while not shutdown_event.is_set():
            if not consumer_thread.is_alive():
                logger.error("Consumer thread is dead; exiting process")
                os._exit(1)
            time.sleep(10)

    watcher_thread = threading.Thread(target=watch_consumer, daemon=True)
    watcher_thread.start()

    return consumer_thread, watcher_thread


@asynccontextmanager
async def lifespan(app: FastAPI):
    shutdown_event = threading.Event()
    consumer_state = ConsumerState()
    app.state.consumer_state = consumer_state
    app.state.shutdown_event = shutdown_event

    consumer_thread, watcher_thread = _start_consumer_threads(
        shutdown_event, consumer_state
    )
    logger.info("Keep reconciler started")
    try:
        yield
    finally:
        if not shutdown_event.is_set():
            logger.info("Shutting down consumer...")
            shutdown_event.set()
        consumer_thread.join(timeout=30)
        watcher_thread.join(timeout=5)
        logger.info("Keep reconciler stopped")
