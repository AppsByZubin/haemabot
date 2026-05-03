#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        constants.py
 Author:      Amit Mohanty

 Notes:
    - define all constants here.
==================================================
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]


def _repo_path(*parts: str) -> str:
    return str(BASE_DIR.joinpath(*parts))


TREND_FILE = _repo_path("files", "trend.json")
NIFTY50_OPTION_CONTRACTS_FILE = _repo_path("files", "nifty50_option_contracts.json")
UPSTOX_NSE_INSTRUMENT_FILE = _repo_path("files", "upstox_nse_instruments.json")
HOLIDAY_LIST_FILE = _repo_path("files", "holiday_list.json")
PARAM_PATH = _repo_path("files", "param.yaml")

NIFTY50 = "nifty50"
NIFTY50_SYMBOL = "NSE_INDEX|Nifty 50"

SUCCESS = "success"
FAIL = "fail"
COMPLETE = "complete"
SUCCESS_CODE = 1
FAIL_CODE = 0

MARKET_START_MINUTE = 91500
MARKET_END_MINUTE = 153000

BULLISH = "bullish"
BEARISH = "bearish"
SIDEWAYS = "sideways"
SUPER_BULLISH = "super_bullish"
SUPPER_BEARISH = "super_bearish"
GAP_UP = "GAP_UP"
GAP_DOWN = "GAP_DOWN"
FLAT = "FLAT"

PROD_FOLDER_PATH = _repo_path("files", "execution_results", "prod")
ORDER_PROD_EVENT_LOG = _repo_path("files", "execution_results", "prod", "order_event_log.json")
ORDER_PROD_LOG = _repo_path("files", "execution_results", "prod", "order_log.csv")
ORDER_PROD_STATUS_LOG = _repo_path("files", "execution_results", "prod", "order_status_log.csv")
DAILY_PROD_PNL = _repo_path("files", "execution_results", "prod", "daily_pnl.csv")

MOCK_FOLDER_PATH = _repo_path("files", "execution_results", "mock")
ORDER_MOCK_EVENT_LOG = _repo_path("files", "execution_results", "mock", "order_event_log.json")
ORDER_MOCK_LOG = _repo_path("files", "execution_results", "mock", "order_log.csv")
ORDER_MOCK_STATUS_LOG = _repo_path("files", "execution_results", "mock", "order_status_log.csv")
DAILY_MOCK_PNL = _repo_path("files", "execution_results", "mock", "daily_pnl.csv")

SANDBOX_FOLDER_PATH = _repo_path("files", "execution_results", "sandbox")
ORDER_SANDBOX_EVENT_LOG = _repo_path("files", "execution_results", "sandbox", "order_event_log.json")
ORDER_SANDBOX_LOG = _repo_path("files", "execution_results", "sandbox", "order_log.csv")
ORDER_SANDBOX_STATUS_LOG = _repo_path("files", "execution_results", "sandbox", "order_status_log.csv")
DAILY_SANDBOX_PNL = _repo_path("files", "execution_results", "sandbox", "daily_pnl.csv")

ORDER_EVENT_LOG = _repo_path("files", "order_event_log.json")
ORDER_LOG = _repo_path("files", "order_log.csv")
DAILY_PNL = _repo_path("files", "daily_pnl.csv")

UPSTOX_API_ACCESS_TOKEN = "upstox_api_access_token"
UPSTOX_SANDBOX_API_ACCESS_TOKEN = "upstox_sandbox_api_access_token"
UPSTOX_NSE_INSTRUMENT_FQDN = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# Execution mode
SANDBOX = "sandbox"
MOCK = "mock"
PRODUCTION = "production"

# Params
SMA_LONG = 20
SMA_SHORT = 5

# Strategy constants
SMA_CROSSOVER = "sma_crossover"
RSI_EMA_ANGLE = "rsi_ema_angle"
PCR_VWAP_EMA = "pcr_vwap_ema"
HM_EMA_ADX = "hm_ema_adx"

BUY = "BUY"
SELL = "SELL"
SL = "SL"
SL_M = "SL-M"
MARKET = "MARKET"

CALL = "CALL"
CE = "CE"
PUT = "PUT"
PE = "PE"
WAITING = "WAITING"
OPEN = "OPEN"
TARGET_HIT = "TARGET HIT"
STOPLOSS_HIT = "STOPLOSS HIT"
MANUAL_EXIT = "MANUAL EXIT"
EOD_SQUARE_OFF = "EOD_SQUARE_OFF"
