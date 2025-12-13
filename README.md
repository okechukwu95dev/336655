# GitHub Actions - 365Scores Parallel Fetch

Automated parallel fetcher for 365Scores games using GitHub Actions.

## Features

- **10 concurrent workers** for fast parallel processing
- **Date-range driven**: Specify start/end dates at trigger time
- **Incremental fetching**: Appends to existing DB (base64 encoded)
- **Full context**: Games + H2H + Form + Squads + Standings
- **Error resilient**: Retries, timeouts, graceful degradation
- **Logging**: JSON structured logs for debugging
- **Manual trigger**: No scheduled runs, full control

## Setup

1. Create new repo `github-actions-fetch-365`
2. Copy all files from this directory
3. Commit and push to GitHub
4. Ensure `games.db.b64` exists (can be empty initially)

## Usage

### Via GitHub UI

1. Navigate to **Actions** → **Fetch Games**
2. Click **Run workflow**
3. Enter start date (e.g., `12/12/2025`)
4. Enter end date (e.g., `22/12/2025`)
5. Click **Run workflow**

### Via GitHub CLI

```bash
gh workflow run fetch_games.yml \
  -f start_date=12/12/2025 \
  -f end_date=22/12/2025
```

## How It Works

1. **Restore**: Decodes `games.db.b64` to `games.db`
2. **Initialize**: Creates tables if new
3. **Discover**: Fetches root games for date range
4. **Process**: 10 workers recursively fetch:
   - Depth 0 (ROOT): Full details, H2H, form, squads, standings
   - Depth 1 (LEAF): Details only
5. **Encode**: Converts `games.db` to base64
6. **Push**: Commits `games.db.b64` and pushes

## Performance

- **558 games** (~8-12 minutes)
- **10x faster** than sequential
- Rate-limited to 0.1s/request (API friendly)

## Logs

All runs create timestamped JSON logs in `logs/`.
Artifacts are available in workflow run details.

## Database

SQLite DB is stored as base64-encoded text in `games.db.b64`.
This allows Git to track binary changes cleanly.

To decode locally:
```bash
base64 -d games.db.b64 > games.db
sqlite3 games.db "SELECT COUNT(*) FROM games;"
```

## Troubleshooting

### Push fails
- Check GITHUB_TOKEN has `contents:write` permission
- Verify branch protection rules

### DB restore fails
- Delete `games.db.b64` to start fresh
- Next run will create new DB

### API rate limiting
- Logs will show `RATE_LIMITED` events
- Adjust `RATE_LIMIT_SECONDS` in script

## License

MIT
