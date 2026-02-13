# NBA Pinnacle-Polymarket Monitor

Real-time monitoring pipeline that detects pricing gaps between Pinnacle (efficient sportsbook) and Polymarket (prediction market) for NBA games.

## Thesis

> When Pinnacle moves a line, Polymarket prices lag behind. This gap creates a window of opportunity.

```
Profit = sum(lag_time * price_distortion) for each trigger event
```

This system performs **forward validation** of this hypothesis by collecting real-time data from both markets and measuring how quickly Polymarket converges to Pinnacle's price signals.

## Architecture

```
                      WebSocket Mode (recommended)

 [Polymarket WebSocket] ──realtime──> [Anomaly Detector]
          |                                   |
          |                          (on anomaly only)
          |                                   v
          |                          [Pinnacle Oracle]
          |                                   |
          v                                   v
    [SQLite DB] <──────────────────── [Gap Recorder]
          |                                   |
          v                                   v
   [Bot Trade Checker]              [Hi-Res Capture]
          |                           (t+3s, t+10s, t+30s)
          v                                   v
   [Paper Trading]                  [Alert / Report]
```

Two modes of operation:

| | REST Polling | WebSocket |
|---|---|---|
| Polymarket | 30s REST polling | WebSocket realtime |
| Pinnacle | 1hr interval | On anomaly only |
| Odds API credits | ~400-600/mo | ~100/mo |
| Reaction latency | up to 30s | <1s |

## Key Results (12 days of data)

**Hi-Res Gap Analysis** -- grade: **A (promising)**

| Timepoint | % with gap >= 4%p | Avg \|gap\| |
|---|---|---|
| t+0s | 63.7% | 14.08% |
| t+3s | 52.6% | 9.91% |
| t+10s | 50.1% | 9.32% |
| t+30s | 48.1% | 9.99% |

Over half of oracle moves still show 4%p+ gap after 3 seconds -- execution is theoretically possible.

## Project Structure

```
.
├── src/                        # Core library
│   ├── clients/                # API clients (CLOB, Gamma, Odds, WS)
│   ├── db/                     # SQLite repository layer
│   ├── shared/                 # NBA mappings, time utils, math
│   ├── strategies/
│   │   ├── lag/                # Lag arbitrage strategy
│   │   └── rebalance/          # Rebalance strategy
│   └── config.py
├── scripts/                    # Execution entrypoints
│   ├── run_lag.py              # Lag monitor runner
│   ├── run_rebalance.py        # Rebalance monitor runner
│   └── setup_ec2.sh            # EC2 initial setup
├── tools/                      # Standalone analysis tools
│   ├── collector/              # Polymarket user data collector
│   ├── backtest/               # Historical backtesting scripts
│   ├── analyze_wallet.py
│   ├── arb_scanner.py
│   └── ...
├── tests/                      # Test suite
├── docker-compose.yml          # Docker services
├── Dockerfile
└── .github/workflows/deploy.yml
```

## Quick Start

### Prerequisites

- Python 3.11+
- [The Odds API](https://the-odds-api.com/) key (free tier: 500 req/mo)

### Local Setup

```bash
git clone https://github.com/bucks-maker/NBA_Monitoring.git
cd NBA_Monitoring

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env and add your ODDS_API_KEY
```

### Run

```bash
# Lag monitor — WebSocket mode (recommended)
PYTHONPATH=. python scripts/run_lag.py --ws

# Rebalance monitor
PYTHONPATH=. python scripts/run_rebalance.py
```

### Docker (EC2 Deployment)

```bash
# Initial setup on Amazon Linux 2023
bash scripts/setup_ec2.sh

# Edit .env with your API keys
vi .env

# Start services
docker compose up -d --build
```

The `docker-compose.yml` runs two services:
- **lag-monitor**: WebSocket-based real-time gap detector
- **rebalance-monitor**: Multi-outcome arbitrage scanner

## Data Sources

| Source | API | Cost |
|---|---|---|
| Pinnacle odds | [The Odds API](https://the-odds-api.com/) | Free tier 500 req/mo |
| Polymarket prices | [Gamma API](https://gamma-api.polymarket.com) | Free, unlimited |
| Polymarket orderbook | [CLOB WebSocket](https://docs.polymarket.com/) | Free, unlimited |
| Bot trades | Polymarket Activity API | Free |

## Database

SQLite with the following tables:

| Table | Description | Rows (sample) |
|---|---|---|
| `pinnacle_snapshots` | Pinnacle line/odds snapshots | 17,591 |
| `poly_snapshots` | Polymarket price/orderbook snapshots | 57,607 |
| `triggers` | Detected line move triggers | 454 |
| `move_events_hi_res` | Hi-res gap events (t0~t30s) | 1,192 |
| `gap_series_hi_res` | 1-second gap time series | 4,216 |
| `paper_trades` | Simulated paper trades | 14,049 |
| `bot_trades` | Tracked bot (0x6e82) trades | 773 |
| `game_mapping` | Pinnacle-Polymarket game mapping | 98 |

## Anomaly Detection Triggers

Model-free, strong signals only (minimize false positives):

```python
# 1. Price spike (5-min window)
if abs(current_price - price_5min_ago) >= 0.05:  # 5%p
    call_pinnacle()

# 2. Thin orderbook
if (best_ask - best_bid) >= 0.05:  # 5%p spread
    call_pinnacle()

# 3. Yes/No sum mismatch
if abs(1.0 - (yes_price + no_price)) >= 0.03:  # 3%p
    call_pinnacle()
```

Pinnacle cooldown: 30 min per game to conserve API credits.

## Environment Variables

```env
ODDS_API_KEY=your_odds_api_key_here
BOT_ADDRESS=0x_target_bot_address_here  # optional
```

## Tests

```bash
PYTHONPATH=. pytest tests/ -v
```

## CI/CD

Push to `main` triggers:
1. Run test suite on GitHub Actions
2. SSH deploy to EC2 (pull + docker compose rebuild)

## License

MIT
