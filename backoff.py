def total_backoff(retries):
    """Total seconds waited across all retries with 2 ** attempt backoff."""
    total = 0
    for attempt in range(retries):
        total =+ 2 ** attempt
    return total
