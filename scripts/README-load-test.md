# Load Testing with Locust

Simulates concurrent flash sale traffic against the Saga Initiator API.

## Prerequisites

```bash
pip install locust
```

## Configuration

Set the API Gateway URL and flash sale event ID via environment variables:

```bash
export API_GATEWAY_URL="https://your-api-id.execute-api.us-east-1.amazonaws.com/prod"
export FLASH_SALE_EVENT_ID="11111111-1111-1111-1111-111111111111"
```

Defaults: `https://api.flash-sale.example.com` and `11111111-1111-1111-1111-111111111111`.

## Run Locally (Interactive Web UI)

```bash
locust -f scripts/load_test.py --host "$API_GATEWAY_URL"
```

Open http://localhost:8089, set the number of users and spawn rate, then start.

## Run Headless (Automated / CI)

### Smoke test — 10 users, 30 seconds

```bash
locust -f scripts/load_test.py \
  --host "$API_GATEWAY_URL" \
  --headless \
  --users 10 \
  --spawn-rate 1 \
  --run-time 30s \
  --csv results/smoke_test
```

### Flash sale burst — 1,000 users, 100 spawn rate, 60 seconds

```bash
locust -f scripts/load_test.py \
  --host "$API_GATEWAY_URL" \
  --headless \
  --users 1000 \
  --spawn-rate 100 \
  --run-time 60s \
  --csv results/flash_sale
```

### Stress test — 5,000 users, 500 spawn rate, 3 minutes

```bash
locust -f scripts/load_test.py \
  --host "$API_GATEWAY_URL" \
  --headless \
  --users 5000 \
  --spawn-rate 500 \
  --run-time 180s \
  --csv results/stress
```

### Soak test — 200 users, 20 spawn rate, 30 minutes

```bash
locust -f scripts/load_test.py \
  --host "$API_GATEWAY_URL" \
  --headless \
  --users 200 \
  --spawn-rate 20 \
  --run-time 30m \
  --csv results/soak
```

## Output

The `--csv` flag writes three files per run:

- `results/<name>_stats.csv` — per-endpoint latency and failure counts
- `results/<name>_stats_history.csv` — time-series of stats
- `results/<name>_failures.csv` — individual failure details

## Post-Test Verification

After every load test execute against a deployed environment, run the reconciliation queries in Section 6.3.4 of `docs/SYSTEM_DESIGN_DOCUMENT.md` to verify zero overselling and zero double charges.
