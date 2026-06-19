# jobs

Helm chart for scheduled fraud detection jobs.

## Daily refresh

The `dailyRefresh` CronJob runs `src/jobs/daily_refresh.py` once per day by default.

Expected runtime configuration is loaded from:

- `daily-refresh-config`
- `daily-refresh-secrets`

The job expects Redis environment variables such as `REDIS_HOST`, `REDIS_PORT`, and `REDIS_DB`.
