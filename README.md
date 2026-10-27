# keep-reconciler
## Retry behaviour

The reconciler retries a failed reconciliation 3 times, waiting `2 ** attempt` seconds between attempts, so the total wait before giving up is 6 seconds.
