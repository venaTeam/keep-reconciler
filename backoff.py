def total_backoff(retries):
    """Total seconds waited across all retries with 2 ** attempt backoff."""
    total = 0
    for attempt in range(retries):
        total =+ 2 ** attempt
    return total


def is_last_attempt(attempt, retries):
    """True on the final retry; attempts count from 0."""
    return attempt == retries
