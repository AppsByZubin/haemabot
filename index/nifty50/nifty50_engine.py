import os
import sys
import threading
import time as t
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
import upstox_client

import common.constants as constants
from logger import create_logger
from broker.upstox_helper import UpstoxHelper
from order_manager.upstox_order_system import UpstoxOrderManager
from order_summary import generate_summary
from index.nifty50.nifty_utils import (
    premarket,
    get_instrument_intraday_data,
    get_option_contracts,
    get_nifty_option_instruments,
    get_spot_price
)
from utils.generic_utils import (
    save_ohlc_to_json,
    get_previous_day_trend,
)

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("Nifty50EngineLogger")


def _coerce_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in {"1", "true", "yes", "y", "on"}:
            return True
        if norm in {"0", "false", "no", "n", "off"}:
            return False
    return default


def nifty50_engine(strategy, mode, param_data):
    """
    Fixes:
    1) Close websocket if idle (no messages) for N seconds (watchdog).
    2) Auto-reconnect websocket on transient disconnects (remote host lost, network blips).
       IMPORTANT: transient WS close SHOULD NOT stop PCR poller / bot.
    3) Cleanup stops option polling (bot.stop()) + disconnect streamer only on FINAL exit.
    4) Correct callback signatures and keep main thread alive.

    Notes:
    - Set strategy-parameters.ws_reconnect_enabled=false to disable reconnect behavior (falls back to old stop-on-close).
    - Backoff is exponential with caps.
    """

    sp = {}
    try:
        sp = (param_data or {}).get("strategy-parameters", {}) if isinstance(param_data, dict) else {}
    except Exception:
        sp = {}

    def _get_strategy_param(*names, default=None):
        sources = [sp]
        if isinstance(param_data, dict):
            sources.append(param_data)
        for source in sources:
            if not isinstance(source, dict):
                continue
            for name in names:
                if name in source:
                    return source.get(name)
        return default

    oil_track_enabled = _coerce_bool(
        _get_strategy_param("OIL_TRACK", "oil_track", default=False),
        False,
    )

    WS_IDLE_TIMEOUT_SEC = int(sp.get("ws_idle_timeout_sec", 20))
    WATCHDOG_CHECK_EVERY_SEC = int(sp.get("ws_watchdog_check_sec", 5))

    # reconnect controls
    WS_RECONNECT_ENABLED = bool(sp.get("ws_reconnect_enabled", True))
    WS_RECONNECT_BASE_SEC = float(sp.get("ws_reconnect_base_sec", 1.5))
    WS_RECONNECT_MAX_SEC = float(sp.get("ws_reconnect_max_sec", 60.0))

    # ensure pcr polling interval is set (strategy may read it)
    try:
        sp["pcr_poll_interval_sec"] = int(sp.get("pcr_poll_interval_sec", 30))
    except Exception:
        pass

    streamer = None
    bot = None

    ws_closed_event = threading.Event()
    stop_event = threading.Event()

    cleanup_lock = threading.Lock()
    cleanup_done = {"flag": False}

    last_msg_lock = threading.Lock()
    last_msg_epoch = {"t": t.time()}

    watchdog_stop = threading.Event()

    # used to coordinate reconnects
    reconnect_lock = threading.Lock()
    reconnect_reason = {"reason": None}

    def _streamer_disconnect_safe():
        nonlocal streamer
        if streamer is None:
            return
        try:
            if hasattr(streamer, "disconnect") and callable(getattr(streamer, "disconnect")):
                streamer.disconnect()
            elif hasattr(streamer, "close") and callable(getattr(streamer, "close")):
                streamer.close()
        except Exception as e:
            logger.warning(f"Error while disconnecting streamer: {e}")

    def cleanup(reason: str = "", final: bool = False):
        """
        final=False:
          - disconnect streamer
          - set ws_closed_event so reconnect loop can proceed
          - DO NOT stop bot (keeps PCR poller alive)

        final=True (run once):
          - stop watchdog
          - stop PCR poller (bot.stop)
          - disconnect streamer
          - set ws_closed_event
        """
        if final:
            with cleanup_lock:
                if cleanup_done["flag"]:
                    return
                cleanup_done["flag"] = True

        if reason:
            logger.info(f"[CLEANUP]{' FINAL' if final else ''} {reason}")
        else:
            logger.info(f"[CLEANUP]{' FINAL' if final else ''} starting")

        if final:
            try:
                watchdog_stop.set()
            except Exception:
                pass

            try:
                if bot is not None and hasattr(bot, "stop") and callable(getattr(bot, "stop")):
                    bot.stop()
                    logger.info("Stopped option polling (bot.stop).")
            except Exception as e:
                logger.warning(f"Failed to stop bot polling: {e}")

        _streamer_disconnect_safe()

        ws_closed_event.set()

        if final:
            stop_event.set()
            logger.info("[CLEANUP FINAL] done")
        else:
            logger.info("[CLEANUP] streamer disconnected (reconnect path)")

    def watchdog_loop():
        logger.info(f"[WATCHDOG] started idle_timeout={WS_IDLE_TIMEOUT_SEC}s check_every={WATCHDOG_CHECK_EVERY_SEC}s")
        while not watchdog_stop.is_set() and not stop_event.is_set():
            try:
                now = t.time()
                with last_msg_lock:
                    last = float(last_msg_epoch["t"])

                if (now - last) >= WS_IDLE_TIMEOUT_SEC:
                    msg = f"[WATCHDOG] No WS message for {int(now - last)}s -> closing WS"
                    logger.warning(msg)
                    with reconnect_lock:
                        reconnect_reason["reason"] = "idle-timeout"
                    # disconnect only; reconnect loop will restore if enabled
                    cleanup("WS idle timeout", final=not WS_RECONNECT_ENABLED)
                    return
            except Exception as e:
                logger.warning(f"[WATCHDOG] error: {e}")

            watchdog_stop.wait(WATCHDOG_CHECK_EVERY_SEC)

        logger.info("[WATCHDOG] stopped")

    try:
        api_token = os.getenv(constants.UPSTOX_API_ACCESS_TOKEN)
        if not api_token:
            logger.error("API token not found. Please set UPSTOX_API_ACCESS_TOKEN.")
            return

        upstox = UpstoxHelper(api_token, is_sandbox=False)
        if upstox is None:
            logger.error("Upstox client not created.")
            sys.exit(constants.FAIL_CODE)

        # order manager
        order_manager = None
        if mode == constants.MOCK:
            from order_manager.mock_order_system import MockOrderManager
            order_manager = MockOrderManager()
            logger.info("Mock mode enabled.")
        elif mode == constants.SANDBOX:
            sandbox_api_token = os.getenv(constants.UPSTOX_SANDBOX_API_ACCESS_TOKEN)
            if not sandbox_api_token:
                logger.error("Sandbox API token not found. Please set UPSTOX_SANDBOX_API_ACCESS_TOKEN.")
                return

            upstox_sandbox_helper = UpstoxHelper(sandbox_api_token)
            order_manager = UpstoxOrderManager(upstox_sandbox_helper, tsl_buffer=float(sp.get("sl_limit_gap", sp.get("sl_limit_gap", 0.5))))
            logger.info("Sandbox mode enabled.")
        elif mode == constants.PRODUCTION:
            upstox_helper = UpstoxHelper(api_token)
            order_manager = UpstoxOrderManager(
                upstox_helper,
                strategy_parameters=sp,
                tsl_buffer=float(sp.get("sl_limit_gap", sp.get("sl_limit_gap", 0.5))),
                orders_csv=constants.ORDER_PROD_LOG,
                daily_csv=constants.DAILY_PROD_PNL,
                events_json_path=constants.ORDER_PROD_EVENT_LOG
            )
        else:
            logger.error("Mode is not set correctly")
            sys.exit(constants.FAIL_CODE)

        # option contracts + premarket
        get_option_contracts(upstox, constants.NIFTY50_SYMBOL)

        ohlc_two_days = premarket(upstox)
        if ohlc_two_days is None:
            logger.error("Failed to fetch premarket data.")
            sys.exit(constants.FAIL_CODE)

        last_day_close=0
        if len(ohlc_two_days) ==2:
            save_ohlc_to_json(ohlc_two_days)

            trend, price_range, last_day_close = get_previous_day_trend()
            logger.info(f"Previous day trend: {trend}")
            logger.info(
                f"Price range: Support-{price_range[0]}, Deep Support-{price_range[1]}, "
                f"Resistance-{price_range[2]}, Deep Resistance-{price_range[3]}"
            )

        future_contract = upstox.get_future_contracts_by_instrument(month_offset=0)
        if future_contract is None:
            logger.error("Failed to fetch upcoming NIFTY future contract.")
            sys.exit(constants.FAIL_CODE)

        oil_contract = None
        if oil_track_enabled:
            oil_contract = upstox.get_crudeoil_future_contract(
                query=str(_get_strategy_param("OIL_QUERY", "oil_query", default="Crudeoil")),
                expiry=str(_get_strategy_param("OIL_EXPIRY", "oil_expiry", default="current_month")),
                exchanges=str(_get_strategy_param("OIL_EXCHANGES", "oil_exchanges", default="MCX")),
                segments=str(_get_strategy_param("OIL_SEGMENTS", "oil_segments", default="FUT")),
                selected_index=int(_get_strategy_param("OIL_SELECT", "oil_select", default=0) or 0),
            )
            logger.info(
                f"Crude oil tracking enabled. instrument_key={oil_contract.get('instrument_key')} "
                f"trading_symbol={oil_contract.get('trading_symbol')}"
            )

        if last_day_close ==0:
            last_day_close = get_spot_price(upstox, constants.NIFTY50, constants.NIFTY50_SYMBOL)

        if last_day_close == 0:
            logger.error("Failed to fetch nifty 50 LTP.")
            sys.exit(constants.FAIL_CODE)

        # expose previous-day close to strategy for gap% calculations
        if isinstance(param_data, dict):
            ht_cfg = param_data.get("historical-trend")
            if not isinstance(ht_cfg, dict):
                ht_cfg = {}
                param_data["historical-trend"] = ht_cfg
            ht_cfg["last-day-close"] = float(last_day_close)

        selected_contracts = {}
        intraday_day_1min_candles = []
        intraday_day_future_candles = []
        intraday_day_oil_candles = []
        minutes_processed = {}
        future_minutes_processed = {}
        oil_minutes_processed = {}

        # time gates
        now_ist = datetime.now(ist)
        current_time = now_ist.time()

        market_start_time = time(9, 15)
        market_end_time = time(15, 30)

        def _select_contracts_from_price(price_value, price_source):
            if price_value is None:
                raise Exception(f"Unable to select contracts because {price_source} price is unavailable.")

            atm_price = int(round(float(price_value) / 50) * 50)
            logger.info(
                f"Selecting option instruments using ATM {atm_price} derived from {price_source} price {price_value}."
            )
            contracts = get_nifty_option_instruments(atm_price, sp.get("trade_expiry"))
            if not isinstance(contracts, dict):
                raise Exception("Option contract selection returned an invalid response.")
            return contracts

        if current_time > market_end_time:
            logger.info("Market already closed for the day. Exiting.")
            sys.exit(constants.SUCCESS_CODE)

        elif current_time < market_start_time:
																		  
            market_start_dt = datetime.combine(now_ist.date(), market_start_time, tzinfo=ist)					  
            wait_time = int((market_start_dt - now_ist).total_seconds()) - 2

            while wait_time > 0:
                logger.debug(f"Waiting for market to open... {wait_time}s remaining")
                t.sleep(min(wait_time, 10))
                wait_time -= 10
			
            spot_price = get_spot_price(upstox, constants.NIFTY50, constants.NIFTY50_SYMBOL)
            if spot_price is None:
                logger.error("Failed to fetch nifty 50 spot price at market open.")
                sys.exit(constants.FAIL_CODE)

            selected_contracts = _select_contracts_from_price(spot_price, "market-open spot")

            logger.info(f"Market open now. with spot price: {spot_price}. Bootstrapping intraday candles from market open.")
        else:
            logger.info("Market already open. Bootstrapping intraday candles.")
            data = get_instrument_intraday_data(upstox, constants.NIFTY50_SYMBOL)
            data.reverse()
            intraday_day_1min_candles.extend(data)

            future_data = get_instrument_intraday_data(upstox, future_contract["instrument_key"])
            future_data.reverse()
            intraday_day_future_candles.extend(future_data)

            if oil_track_enabled and oil_contract is not None:
                oil_data = get_instrument_intraday_data(upstox, oil_contract["instrument_key"])
                oil_data.reverse()
                intraday_day_oil_candles.extend(oil_data)

        # Close the streamer at market end (15:30 IST) and generate a monthly summary.
        def market_close_watcher():
            while not stop_event.is_set():
                now = datetime.now(ist)
                if now.time() >= market_end_time:
                    logger.info("Market end reached (15:30 IST). Shutting down gracefully.")
                    cleanup("market close", final=True)
                    stop_event.set()
                    break
                # wake up frequently enough to close near the boundary
                t.sleep(10)

        threading.Thread(
            target=market_close_watcher,
            name="market_close_watcher",
            daemon=True,
        ).start()

        # build df
        columns = ["time", "open", "high", "low", "close", "volume", "oi"]

        df_nifty = pd.DataFrame()
        if intraday_day_1min_candles:
            df_nifty = pd.DataFrame(intraday_day_1min_candles, columns=columns)
            df_nifty.drop(columns=["volume", "oi"], inplace=True)

            last_timestamp = df_nifty["time"].iloc[-1]
            dt = datetime.fromisoformat(last_timestamp).astimezone(ist)
            minutes_processed[dt.strftime("%Y-%m-%d %H:%M")] = True

            close_price = intraday_day_1min_candles[-1][4]
            selected_contracts = _select_contracts_from_price(close_price, "latest intraday close")
        elif current_time >= market_start_time:
            logger.warning("No intraday NIFTY candles available during bootstrap. Falling back to live spot price.")
            spot_price = get_spot_price(upstox, constants.NIFTY50, constants.NIFTY50_SYMBOL)
            if spot_price is None:
                logger.error("Failed to fetch nifty 50 spot price for bootstrap fallback.")
                sys.exit(constants.FAIL_CODE)
            selected_contracts = _select_contracts_from_price(spot_price, "bootstrap fallback spot")

        df_future = pd.DataFrame()
        if intraday_day_future_candles:
            df_future = pd.DataFrame(intraday_day_future_candles, columns=columns)
            future_last_timestamp = df_future["time"].iloc[-1]
            future_dt = datetime.fromisoformat(future_last_timestamp).astimezone(ist)
            future_minutes_processed[future_dt.strftime("%Y-%m-%d %H:%M")] = True

        df_oil = pd.DataFrame()
        if intraday_day_oil_candles:
            df_oil = pd.DataFrame(intraday_day_oil_candles, columns=columns)
            oil_last_timestamp = df_oil["time"].iloc[-1]
            oil_dt = datetime.fromisoformat(oil_last_timestamp).astimezone(ist)
            oil_minutes_processed[oil_dt.strftime("%Y-%m-%d %H:%M")] = True

        # instruments list
        list_of_instruments = []
        if not isinstance(selected_contracts, dict):
            logger.error("Failed to initialize selected contracts.")
            sys.exit(constants.FAIL_CODE)
        selected_contracts["Nifty_Future"] = future_contract
        if oil_track_enabled and oil_contract is not None:
            selected_contracts["CrudeOil_Future"] = oil_contract

        for key, value in selected_contracts.items():
            if isinstance(value, dict) and value.get("instrument_key"):
                list_of_instruments.append(value["instrument_key"])
                continue
            if isinstance(value, list):
                for contract in value:
                    list_of_instruments.append(contract["instrument_key"])

        if strategy == constants.HM_EMA_ADX:
            from index.nifty50.strategy import HmEmaAdxStrategy

            bot = HmEmaAdxStrategy(
                current_date=now_ist.strftime("%Y%m%d"),
                expiry_date=sp.get("trade_expiry"),
                order_manager=order_manager,
                params=param_data,
                uptox_client=upstox,
                previous_day_trend=trend,
                selected_contracts=selected_contracts,
                index_minutes_processed=minutes_processed,
                future_minutes_processed=future_minutes_processed,
                oil_minutes_processed=oil_minutes_processed,
                intraday_index_candles=intraday_day_1min_candles,
                intraday_future_candles=intraday_day_future_candles,
                intraday_oil_candles=intraday_day_oil_candles,
            )
        else:
            logger.error(f"Strategy {strategy} is not available in haemabot.")
            sys.exit(constants.FAIL_CODE)

        # start PCR poller once (DO NOT stop on transient WS close)
        if hasattr(bot, "start") and callable(getattr(bot, "start")):
            bot.start()

        if hasattr(bot, "get_subscription_instruments") and callable(getattr(bot, "get_subscription_instruments")):
            try:
                strategy_instruments = bot.get_subscription_instruments()
                if isinstance(strategy_instruments, list) and strategy_instruments:
                    list_of_instruments = strategy_instruments
            except Exception as e:
                logger.warning(f"Failed to fetch strategy subscription instruments: {e}")

        # include index
        if constants.NIFTY50_SYMBOL not in list_of_instruments:
            list_of_instruments.append(constants.NIFTY50_SYMBOL)
        logger.info(f"Tracking instruments: {list_of_instruments}")

        # streamer client
        configuration = upstox_client.Configuration()
        configuration.access_token = api_token

													  
        api_client = upstox_client.ApiClient(configuration)
								
				   
        def on_message(message, *args, **kwargs):
            with last_msg_lock:
                last_msg_epoch["t"] = t.time()
            try:
                bot.on_ws_message(message)
            except Exception as e:
                logger.warning(f"bot.on_ws_message error ({type(e).__name__}): {e}", exc_info=True)

        def on_error(error, *args, **kwargs):
													  
            logger.warning(f"WebSocket Streaming error: {error}")
            with reconnect_lock:
                reconnect_reason["reason"] = str(error)
            # disconnect streamer; reconnect loop will handle
            if WS_RECONNECT_ENABLED:
                cleanup(f"streamer error: {error}", final=False)
            else:
                cleanup(f"streamer error: {error}", final=True)

        def on_close(*args, **kwargs):
            logger.info(f"WebSocket Streaming closed args={args} kwargs={kwargs}")
            with reconnect_lock:
                if reconnect_reason["reason"] is None:
                    reconnect_reason["reason"] = "on_close"
            if WS_RECONNECT_ENABLED:
                cleanup("streamer on_close", final=False)
            else:
                cleanup("streamer on_close", final=True)

        has_connected_once = {"flag": False}

        def on_open(*args, **kwargs):
            logger.info(f"WebSocket Streaming opened args={args} kwargs={kwargs}")
            with last_msg_lock:
                last_msg_epoch["t"] = t.time()
            with reconnect_lock:
                reconnect_reason["reason"] = None
            was_reconnect = has_connected_once["flag"]
            has_connected_once["flag"] = True
            if was_reconnect and hasattr(bot, "on_ws_reconnected") and callable(getattr(bot, "on_ws_reconnected")):
                try:
                    bot.on_ws_reconnected()
                except Exception as e:
                    logger.warning(f"bot.on_ws_reconnected error ({type(e).__name__}): {e}", exc_info=True)
									  
									
        # Start watchdog once; it will disconnect streamer on idle, reconnect loop handles rejoin.
        threading.Thread(target=watchdog_loop, name="ws_watchdog", daemon=True).start()

        # --- reconnect loop ---
        attempt = 0
        while not stop_event.is_set():
            attempt += 1
            ws_closed_event.clear()
										
            streamer = upstox_client.MarketDataStreamerV3(api_client, list_of_instruments, "full")
            streamer.on("message", on_message)
            streamer.on("error", on_error)
            streamer.on("close", on_close)
            streamer.on("open", on_open)

            try:
                logger.info(f"[WS] connect attempt={attempt} reconnect_enabled={WS_RECONNECT_ENABLED}")
                streamer.connect()
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received.")
                cleanup("KeyboardInterrupt", final=True)
                break
            except Exception as e:
                logger.error(f"streamer.connect failed: {e}")
                with reconnect_lock:
                    reconnect_reason["reason"] = f"connect failed: {e}"
                if not WS_RECONNECT_ENABLED:
                    cleanup("connect failed", final=True)
                    break
                cleanup("connect failed (reconnect)", final=False)

            # Wait for close signal (on_close / cleanup)
            while not ws_closed_event.is_set() and not stop_event.is_set():
                t.sleep(1)

            if stop_event.is_set():
                break

            if not WS_RECONNECT_ENABLED:
                cleanup("engine exit (no reconnect)", final=True)
                break

            # Backoff and reconnect
            reason = None
            with reconnect_lock:
                reason = reconnect_reason["reason"]
            backoff = min(WS_RECONNECT_MAX_SEC, WS_RECONNECT_BASE_SEC * (2 ** min(attempt, 6)))
            logger.warning(f"[WS] disconnected reason={reason!r}. Reconnecting in {backoff:.1f}s")
            t.sleep(backoff)

        # final safety
        cleanup("engine exit", final=True)

        # At end of day, print a monthly summary based on logs.
        try:
            generate_summary(order_manager.orders_csv, order_manager.daily_csv)
        except Exception as e:
            logger.warning(f"Failed to generate summary: {e}")

    except Exception as e:
        logger.error(f"An error occurred in nifty50 engine: {e}")
        try:
            cleanup("exception", final=True)
        except Exception:
            pass
        sys.exit(constants.FAIL_CODE)
