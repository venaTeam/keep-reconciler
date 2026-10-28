def is_last_attempt(attempt, retries):
    """True on the final retry; attempts count from 0."""
    return attempt == retries
