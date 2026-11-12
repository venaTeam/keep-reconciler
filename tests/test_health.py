import pytest
from fastapi.testclient import TestClient

from src.app import app
from src.state import ConsumerState


@pytest.fixture
def client():
    # Not used as a context manager, so the lifespan (and the Kafka consumer
    # thread) never starts.
    yield TestClient(app)
    if hasattr(app.state, "consumer_state"):
        del app.state.consumer_state


def set_state(state: ConsumerState):
    app.state.consumer_state = state


def test_starting(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "starting"}


def test_ok(client):
    set_state(ConsumerState())
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_backoff_stays_200(client):
    state = ConsumerState()
    state.record_backoff()
    set_state(state)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "backoff"


def test_crashed_consumer_returns_503(client):
    state = ConsumerState()
    state.mark_unhealthy()
    set_state(state)
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["status"] == "degraded"


def test_success_recovers_from_crash(client):
    state = ConsumerState()
    state.mark_unhealthy()
    state.record_success()
    set_state(state)
    assert client.get("/health").status_code == 200
