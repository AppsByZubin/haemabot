# HaemaBot

HaemaBot is a Python trading bot for NIFTY 50 options. It uses Upstox market data, builds intraday candles from live ticks, applies the `hm_ema_adx` strategy, and routes orders through mock, sandbox, or production order managers.

## Features

- NIFTY 50 intraday workflow with market-open bootstrapping.
- Upstox WebSocket streaming with idle watchdog and reconnect handling.
- Option contract discovery and ATM contract selection.
- `hm_ema_adx` strategy using EMA, ADX, RSI, ATR, gap context, and trade-window rules.
- Mock, sandbox, and production execution modes.
- Order logs, event logs, daily PnL logs, and summary utilities.

## Project Layout

```text
.
|-- main.py                         # CLI entry point for the bot
|-- main_order.py                   # Order-manager test harness
|-- common/constants.py             # Shared constants and file paths
|-- files/param.yaml                # Strategy and runtime configuration
|-- index/orchestrator.py           # Top-level instrument orchestrator
|-- index/nifty50/nifty50_engine.py # NIFTY 50 live engine
|-- index/nifty50/strategy/         # Strategy implementations
|-- order_manager/                  # Mock and Upstox order managers
|-- broker/                         # Upstox helper wrappers
|-- analysis/                       # PnL analysis helpers
`-- order_summary.py                # Trade and daily PnL summary utility
```

## Requirements

- Python 3.10 or newer.
- Upstox API access for live market data and order placement.
- Python packages listed in `requirements.txt`.
- Native TA-Lib libraries may be required by the `TA-Lib` Python package.

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure Upstox credentials. The code reads these exact environment variable names:

```bash
export upstox_api_access_token="your-live-access-token"
export upstox_sandbox_api_access_token="your-sandbox-access-token"
```

For mock mode, only market data access is required. Sandbox mode also requires `upstox_sandbox_api_access_token`. Production mode places real orders through the production Upstox account.

## Configuration

Runtime parameters live in `files/param.yaml`.

Important settings include:

- `trade-per-day` and `max-open-trades`
- `take-profit`, `stop-loss`, and trailing stop settings
- `trade_expiry` and `pcr_expiry`
- ATR, ADX, RSI, EMA, and trend thresholds
- `trade-window.start` and `trade-window.end`
- `historical-trends` market context

Review this file before each session, especially expiry dates, lot sizing, and production risk limits.

## Running

Run the bot with:

```bash
python main.py -i nifty50 -s hm_ema_adx -l mock
```

Available execution modes:

```text
mock
sandbox
production
```

Examples:

```bash
python main.py --instruments nifty50 --strategy hm_ema_adx --level mock
python main.py --instruments nifty50 --strategy hm_ema_adx --level sandbox
python main.py --instruments nifty50 --strategy hm_ema_adx --level production
```

## Logs and Outputs

Console and file logs are written under `logs/`.

Execution results are written under:

```text
files/execution_results/mock/
files/execution_results/sandbox/
files/execution_results/prod/
```

Typical output files include:

- `order_log.csv`
- `order_event_log.json`
- `order_status_log.csv`
- `daily_pnl.csv`

These generated execution result folders are ignored by git.

## Summaries and Analysis

Generate trade and daily PnL summaries:

```bash
python order_summary.py \
  --orders-csv files/execution_results/prod/order_log.csv \
  --daily-csv files/execution_results/prod/daily_pnl.csv
```

Run the plotting analysis helper:

```bash
python analysis/pnl_analysis.py \
  --orders-csv files/execution_results/mock/order_log.csv \
  --daily-csv files/execution_results/mock/daily_pnl.csv \
  --events-json files/execution_results/mock/order_event_log.json
```

Add `--no-plots` if you only want printed stats.

## Safety Notes

This project can place real market orders in `production` mode. Test changes in `mock` or `sandbox` mode first, verify `files/param.yaml`, and confirm the active access token before running production.
