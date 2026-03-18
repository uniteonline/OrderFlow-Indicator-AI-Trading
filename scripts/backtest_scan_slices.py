#!/usr/bin/env python3
"""
Backtest scan trend and range outputs against future 1m price slices.

The script:
1. Reads scan outputs from `journalctl -u orderflow-llm` ENTRY_SCAN logs.
2. Loads 1m futures klines from Postgres using the DB settings in config.yaml.
3. Builds future slices for 15m / 4h / 1d from each scan trigger timestamp.
4. Labels each slice as Bullish / Bearish / Sideways using 1m OHLC path.
5. Uses the future slice min/max as the realized support/resistance range.
6. Scores direction accuracy and range accuracy with a trigger-price tolerance.

Labeling rule:
- If price materially explores both directions away from the trigger price within
  the slice, label it Sideways.
- Otherwise prefer the final close-vs-trigger move.
- If the close is still near the trigger, fall back to the one-sided excursion.

This is intentionally simple and tunable so it can be used for quick scan
backtests and threshold sweeps.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import yaml


TIMEFRAME_MINUTES = {
    "15m": 15,
    "4h": 240,
    "1d": 1440,
}

CANONICAL_TRENDS = {
    "bullish": "Bullish",
    "bearish": "Bearish",
    "sideways": "Sideways",
}


@dataclass(frozen=True)
class ScanRecord:
    ts_bucket: datetime
    trigger_minute: int
    trends: dict[str, str | None]
    ranges: dict[str, dict[str, float | None]]


@dataclass(frozen=True)
class Bar:
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest ENTRY_SCAN trend/range outputs against future 1m price slices."
    )
    parser.add_argument(
        "--config",
        default="/data/config/config.yaml",
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--service",
        default="orderflow-llm",
        help="systemd service name used for journalctl",
    )
    parser.add_argument(
        "--since",
        required=True,
        help="journal/db lower bound, for example '2026-03-17 13:06:15 UTC'",
    )
    parser.add_argument(
        "--until",
        default=None,
        help="journal/db upper bound, for example '2026-03-18 02:45:00 UTC'",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Override symbol from config llm.symbol",
    )
    parser.add_argument(
        "--market",
        default="futures",
        help="Market enum value in md.kline_bar, default futures",
    )
    parser.add_argument(
        "--venue",
        default="binance",
        help="Venue in md.kline_bar, default binance",
    )
    parser.add_argument(
        "--trigger-minutes",
        default=None,
        help="Comma-separated trigger minute buckets. Default reads llm.call_schedule_minutes or 0,15,30,45",
    )
    parser.add_argument(
        "--material-move-bps",
        type=float,
        default=25.0,
        help="Material excursion threshold in bps used for up/down breach detection. Default 25",
    )
    parser.add_argument(
        "--close-move-bps",
        type=float,
        default=None,
        help="Final close threshold in bps. Default uses --material-move-bps",
    )
    parser.add_argument(
        "--range-tolerance-pct",
        type=float,
        default=1.0,
        help="Absolute support/resistance tolerance as a percent of trigger price. Default 1.0",
    )
    parser.add_argument(
        "--output-dir",
        default="/data/tmp/scan_slice_backtest",
        help="Directory for detail/summary CSV output",
    )
    parser.add_argument(
        "--print-limit",
        type=int,
        default=12,
        help="How many detail rows to print to stdout. Default 12",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def parse_trigger_minutes(raw: str | None, config: dict[str, Any]) -> list[int]:
    if raw:
        minutes = [int(part.strip()) for part in raw.split(",") if part.strip()]
    else:
        minutes = config.get("llm", {}).get("call_schedule_minutes", [0, 15, 30, 45])
    return sorted({minute % 60 for minute in minutes})


def normalize_trend(value: Any) -> str | None:
    if value is None:
        return None
    normalized = CANONICAL_TRENDS.get(str(value).strip().lower())
    return normalized


def parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_utc_timestamp(value: str) -> datetime:
    cleaned = value.strip().replace(" UTC", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S+00")


def run_command(args: list[str], env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n{result.stderr.strip()}"
        )
    return result.stdout


def fetch_journal(service: str, since: str, until: str | None) -> str:
    cmd = ["sudo", "journalctl", "-u", service, "--since", since, "--no-pager", "-o", "cat"]
    if until:
        cmd.extend(["--until", until])
    return run_command(cmd)


def parse_entry_scan_records(journal_text: str, allowed_minutes: set[int]) -> list[ScanRecord]:
    lines = journal_text.splitlines()
    records: list[ScanRecord] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.startswith("ENTRY_SCAN ts_bucket="):
            index += 1
            continue

        prefix = "ENTRY_SCAN ts_bucket="
        ts_text = line[len(prefix) :].split(" UTC", 1)[0]
        ts_bucket = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        if ts_bucket.minute not in allowed_minutes:
            index += 1
            continue

        payload_lines: list[str] = []
        cursor = index + 1
        parsed_payload: dict[str, Any] | None = None
        while cursor < len(lines):
            payload_lines.append(lines[cursor])
            joined = "\n".join(payload_lines).strip()
            if joined:
                try:
                    candidate = json.loads(joined)
                except json.JSONDecodeError:
                    cursor += 1
                    continue
                if isinstance(candidate, dict) and "parsed_scan" in candidate:
                    parsed_payload = candidate
                    break
            cursor += 1

        if parsed_payload is None:
            index += 1
            continue

        scan = parsed_payload.get("parsed_scan", {})
        trends = {
            timeframe: normalize_trend(scan.get(timeframe, {}).get("trend"))
            for timeframe in TIMEFRAME_MINUTES
        }
        ranges = {}
        for timeframe in TIMEFRAME_MINUTES:
            range_value = scan.get(timeframe, {}).get("range", {})
            ranges[timeframe] = {
                "support": parse_optional_float(range_value.get("support")),
                "resistance": parse_optional_float(range_value.get("resistance")),
            }
        records.append(
            ScanRecord(
                ts_bucket=ts_bucket,
                trigger_minute=ts_bucket.minute,
                trends=trends,
                ranges=ranges,
            )
        )
        index = cursor + 1
    return records


def build_psql_env(database_cfg: dict[str, Any]) -> dict[str, str]:
    env = dict(os.environ)
    password_key_or_value = str(database_cfg.get("password_env", ""))
    password = os.environ.get(password_key_or_value, password_key_or_value)
    env["PGPASSWORD"] = password
    return env


def query_csv(database_cfg: dict[str, Any], sql: str) -> list[dict[str, str]]:
    cmd = [
        "psql",
        "-h",
        str(database_cfg["host"]),
        "-p",
        str(database_cfg["port"]),
        "-U",
        str(database_cfg["user"]),
        "-d",
        str(database_cfg["database"]),
        "--csv",
        "-c",
        sql,
    ]
    stdout = run_command(cmd, env=build_psql_env(database_cfg))
    return list(csv.DictReader(io.StringIO(stdout)))


def fetch_1m_bars(
    database_cfg: dict[str, Any],
    symbol: str,
    market: str,
    venue: str,
    start_ts: datetime,
    end_ts: datetime,
) -> dict[datetime, Bar]:
    sql = f"""
    SELECT
        open_time,
        close_time,
        open_price,
        high_price,
        low_price,
        close_price
    FROM md.kline_bar
    WHERE symbol = '{symbol}'
      AND market = '{market}'::cfg.market_type
      AND venue = '{venue}'
      AND interval_code = '1m'
      AND is_closed = TRUE
      AND open_time >= '{format_utc_timestamp(start_ts)}'
      AND open_time < '{format_utc_timestamp(end_ts)}'
    ORDER BY open_time
    """
    rows = query_csv(database_cfg, sql)
    bars: dict[datetime, Bar] = {}
    for row in rows:
        open_time = parse_utc_timestamp(row["open_time"])
        bars[open_time] = Bar(
            open_time=open_time,
            close_time=parse_utc_timestamp(row["close_time"]),
            open_price=float(row["open_price"]),
            high_price=float(row["high_price"]),
            low_price=float(row["low_price"]),
            close_price=float(row["close_price"]),
        )
    return bars


def bps_change(a: float, b: float) -> float:
    return ((b / a) - 1.0) * 10_000.0


def pct_from_bps(value: float) -> float:
    return value / 100.0


def expected_slice_times(start_ts: datetime, minutes: int) -> list[datetime]:
    return [start_ts + timedelta(minutes=offset) for offset in range(minutes)]


def first_breach_time(
    bars: Iterable[Bar],
    trigger_price: float,
    material_move_bps: float,
    side: str,
) -> datetime | None:
    if side == "up":
        threshold = trigger_price * (1.0 + material_move_bps / 10_000.0)
        for bar in bars:
            if bar.high_price >= threshold:
                return bar.open_time
        return None
    if side == "down":
        threshold = trigger_price * (1.0 - material_move_bps / 10_000.0)
        for bar in bars:
            if bar.low_price <= threshold:
                return bar.open_time
        return None
    raise ValueError(f"unsupported breach side: {side}")


def classify_slice(
    trigger_ts: datetime,
    timeframe: str,
    bars_by_open_time: dict[datetime, Bar],
    material_move_bps: float,
    close_move_bps: float,
) -> dict[str, Any]:
    horizon_minutes = TIMEFRAME_MINUTES[timeframe]
    expected_times = expected_slice_times(trigger_ts, horizon_minutes)
    bars = [bars_by_open_time.get(ts) for ts in expected_times]
    present_bars = [bar for bar in bars if bar is not None]
    missing_times = [ts for ts, bar in zip(expected_times, bars) if bar is None]

    base: dict[str, Any] = {
        "timeframe": timeframe,
        "horizon_minutes": horizon_minutes,
        "expected_bars": horizon_minutes,
        "actual_bars": len(present_bars),
        "missing_bars": len(missing_times),
        "missing_start_time": missing_times[0].isoformat() if missing_times else None,
        "truth_trend": None,
        "truth_support": None,
        "truth_resistance": None,
        "path_state": "incomplete",
        "start_open": None,
        "end_close": None,
        "max_high": None,
        "min_low": None,
        "close_return_bps": None,
        "close_return_pct": None,
        "max_up_bps": None,
        "max_up_pct": None,
        "max_down_bps": None,
        "max_down_pct": None,
        "up_breach_time": None,
        "down_breach_time": None,
        "complete_window": False,
    }

    if len(present_bars) != horizon_minutes:
        return base

    start_price = present_bars[0].open_price
    end_close = present_bars[-1].close_price
    max_high = max(bar.high_price for bar in present_bars)
    min_low = min(bar.low_price for bar in present_bars)
    close_return_bps = bps_change(start_price, end_close)
    max_up_bps = bps_change(start_price, max_high)
    max_down_bps = bps_change(start_price, min_low)
    down_breach_time = first_breach_time(
        present_bars, start_price, material_move_bps, side="down"
    )
    up_breach_time = first_breach_time(
        present_bars, start_price, material_move_bps, side="up"
    )

    if up_breach_time and down_breach_time:
        if up_breach_time < down_breach_time:
            truth_trend = "Sideways"
            path_state = "up_then_down"
        elif down_breach_time < up_breach_time:
            truth_trend = "Sideways"
            path_state = "down_then_up"
        else:
            truth_trend = "Sideways"
            path_state = "both_same_bar"
    elif close_return_bps >= close_move_bps:
        truth_trend = "Bullish"
        path_state = "close_up"
    elif close_return_bps <= -close_move_bps:
        truth_trend = "Bearish"
        path_state = "close_down"
    elif up_breach_time and not down_breach_time:
        truth_trend = "Bullish"
        path_state = "up_only"
    elif down_breach_time and not up_breach_time:
        truth_trend = "Bearish"
        path_state = "down_only"
    else:
        truth_trend = "Sideways"
        path_state = "no_material_break"

    base.update(
        {
            "truth_trend": truth_trend,
            "truth_support": min_low,
            "truth_resistance": max_high,
            "path_state": path_state,
            "start_open": start_price,
            "end_close": end_close,
            "max_high": max_high,
            "min_low": min_low,
            "close_return_bps": close_return_bps,
            "close_return_pct": pct_from_bps(close_return_bps),
            "max_up_bps": max_up_bps,
            "max_up_pct": pct_from_bps(max_up_bps),
            "max_down_bps": max_down_bps,
            "max_down_pct": pct_from_bps(max_down_bps),
            "up_breach_time": up_breach_time.isoformat() if up_breach_time else None,
            "down_breach_time": down_breach_time.isoformat() if down_breach_time else None,
            "complete_window": True,
        }
    )
    return base


def build_detail_rows(
    scans: list[ScanRecord],
    bars_by_open_time: dict[datetime, Bar],
    material_move_bps: float,
    close_move_bps: float,
    range_tolerance_pct: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scan in scans:
        for timeframe in TIMEFRAME_MINUTES:
            predicted_trend = scan.trends.get(timeframe)
            predicted_range = scan.ranges.get(timeframe, {})
            predicted_support = predicted_range.get("support")
            predicted_resistance = predicted_range.get("resistance")
            slice_metrics = classify_slice(
                trigger_ts=scan.ts_bucket,
                timeframe=timeframe,
                bars_by_open_time=bars_by_open_time,
                material_move_bps=material_move_bps,
                close_move_bps=close_move_bps,
            )
            truth_trend = slice_metrics["truth_trend"]
            complete_window = bool(slice_metrics["complete_window"])
            direction_is_match = (
                complete_window and predicted_trend is not None and predicted_trend == truth_trend
            )
            trigger_price = slice_metrics["start_open"]
            truth_support = slice_metrics["truth_support"]
            truth_resistance = slice_metrics["truth_resistance"]
            range_tolerance_abs = (
                trigger_price * (range_tolerance_pct / 100.0) if trigger_price is not None else None
            )
            support_error_abs = (
                abs(predicted_support - truth_support)
                if predicted_support is not None and truth_support is not None
                else None
            )
            resistance_error_abs = (
                abs(predicted_resistance - truth_resistance)
                if predicted_resistance is not None and truth_resistance is not None
                else None
            )
            support_within_tolerance = (
                support_error_abs is not None
                and range_tolerance_abs is not None
                and support_error_abs <= range_tolerance_abs
            )
            resistance_within_tolerance = (
                resistance_error_abs is not None
                and range_tolerance_abs is not None
                and resistance_error_abs <= range_tolerance_abs
            )
            range_is_match = (
                complete_window
                and predicted_support is not None
                and predicted_resistance is not None
                and support_within_tolerance
                and resistance_within_tolerance
            )
            row = {
                "ts_bucket": scan.ts_bucket.isoformat(),
                "trigger_minute": scan.trigger_minute,
                "timeframe": timeframe,
                "predicted_trend": predicted_trend,
                "truth_trend": truth_trend,
                "direction_is_match": direction_is_match,
                "predicted_support": predicted_support,
                "predicted_resistance": predicted_resistance,
                "range_tolerance_pct": range_tolerance_pct,
                "range_tolerance_abs": range_tolerance_abs,
                "support_error_abs": support_error_abs,
                "resistance_error_abs": resistance_error_abs,
                "support_within_tolerance": support_within_tolerance,
                "resistance_within_tolerance": resistance_within_tolerance,
                "range_is_match": range_is_match,
                **slice_metrics,
            }
            rows.append(row)
    return rows


def summarize(
    rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_timeframe: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_timeframe_and_minute: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        timeframe = str(row["timeframe"])
        trigger_minute = int(row["trigger_minute"])
        by_timeframe[timeframe].append(row)
        by_timeframe_and_minute[(timeframe, trigger_minute)].append(row)

    timeframe_summary: list[dict[str, Any]] = []
    for timeframe in sorted(by_timeframe, key=lambda tf: TIMEFRAME_MINUTES[tf]):
        timeframe_summary.append(summary_row(timeframe=timeframe, trigger_minute=None, rows=by_timeframe[timeframe]))

    minute_summary: list[dict[str, Any]] = []
    for (timeframe, trigger_minute), bucket_rows in sorted(
        by_timeframe_and_minute.items(), key=lambda item: (TIMEFRAME_MINUTES[item[0][0]], item[0][1])
    ):
        minute_summary.append(
            summary_row(timeframe=timeframe, trigger_minute=trigger_minute, rows=bucket_rows)
        )

    return timeframe_summary, minute_summary


def summary_row(
    timeframe: str, trigger_minute: int | None, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    complete_rows = [row for row in rows if row["complete_window"]]
    incomplete_rows = [row for row in rows if not row["complete_window"]]
    direction_comparable_rows = [
        row for row in complete_rows if row["predicted_trend"] is not None and row["truth_trend"] is not None
    ]
    direction_matches = [row for row in direction_comparable_rows if row["direction_is_match"]]
    direction_mismatches = [row for row in direction_comparable_rows if not row["direction_is_match"]]
    direction_accuracy_pct = (
        len(direction_matches) / len(direction_comparable_rows) * 100.0
        if direction_comparable_rows
        else 0.0
    )

    range_comparable_rows = [
        row
        for row in complete_rows
        if row["predicted_support"] is not None
        and row["predicted_resistance"] is not None
        and row["truth_support"] is not None
        and row["truth_resistance"] is not None
    ]
    range_matches = [row for row in range_comparable_rows if row["range_is_match"]]
    range_mismatches = [row for row in range_comparable_rows if not row["range_is_match"]]
    range_accuracy_pct = (
        len(range_matches) / len(range_comparable_rows) * 100.0 if range_comparable_rows else 0.0
    )
    return {
        "timeframe": timeframe,
        "trigger_minute": "" if trigger_minute is None else trigger_minute,
        "total_rows": len(rows),
        "complete_rows": len(complete_rows),
        "incomplete_rows": len(incomplete_rows),
        "direction_comparable_rows": len(direction_comparable_rows),
        "direction_matches": len(direction_matches),
        "direction_mismatches": len(direction_mismatches),
        "direction_accuracy_pct": round(direction_accuracy_pct, 2),
        "range_comparable_rows": len(range_comparable_rows),
        "range_matches": len(range_matches),
        "range_mismatches": len(range_mismatches),
        "range_accuracy_pct": round(range_accuracy_pct, 2),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_table(title: str, rows: list[dict[str, Any]], columns: list[str]) -> None:
    print(f"\n{title}")
    if not rows:
        print("(no rows)")
        return

    widths: dict[str, int] = {}
    for column in columns:
        widths[column] = max(
            len(column),
            max(len(str(row.get(column, ""))) for row in rows),
        )

    header = "  ".join(column.ljust(widths[column]) for column in columns)
    print(header)
    print("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        print("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def main() -> int:
    args = parse_args()
    config = load_yaml(Path(args.config))
    database_cfg = config["database"]
    symbol = args.symbol or config.get("llm", {}).get("symbol") or config.get("indicator", {}).get("symbol")
    if not symbol:
        raise RuntimeError("could not determine symbol from args or config")

    trigger_minutes = parse_trigger_minutes(args.trigger_minutes, config)
    close_move_bps = (
        args.material_move_bps if args.close_move_bps is None else args.close_move_bps
    )

    journal_text = fetch_journal(args.service, args.since, args.until)
    scans = parse_entry_scan_records(journal_text, set(trigger_minutes))
    if not scans:
        raise RuntimeError("no ENTRY_SCAN records found in the requested range")

    earliest_ts = min(record.ts_bucket for record in scans)
    latest_ts = max(record.ts_bucket for record in scans)
    max_horizon_minutes = max(TIMEFRAME_MINUTES.values())
    bars_by_open_time = fetch_1m_bars(
        database_cfg=database_cfg,
        symbol=symbol,
        market=args.market,
        venue=args.venue,
        start_ts=earliest_ts,
        end_ts=latest_ts + timedelta(minutes=max_horizon_minutes),
    )

    rows = build_detail_rows(
        scans=scans,
        bars_by_open_time=bars_by_open_time,
        material_move_bps=args.material_move_bps,
        close_move_bps=close_move_bps,
        range_tolerance_pct=args.range_tolerance_pct,
    )
    timeframe_summary, minute_summary = summarize(rows)

    output_dir = Path(args.output_dir)
    detail_path = output_dir / "scan_slice_detail.csv"
    timeframe_summary_path = output_dir / "scan_slice_summary_by_timeframe.csv"
    minute_summary_path = output_dir / "scan_slice_summary_by_timeframe_minute.csv"

    write_csv(detail_path, rows)
    write_csv(timeframe_summary_path, timeframe_summary)
    write_csv(minute_summary_path, minute_summary)

    print(
        f"Parsed scans={len(scans)} symbol={symbol} trigger_minutes={trigger_minutes} "
        f"material_move_bps={args.material_move_bps} close_move_bps={close_move_bps} "
        f"range_tolerance_pct={args.range_tolerance_pct}"
    )
    print(f"Detail CSV: {detail_path}")
    print(f"Summary CSV: {timeframe_summary_path}")
    print(f"Minute Summary CSV: {minute_summary_path}")

    print_table(
        "Summary By Timeframe",
        timeframe_summary,
        [
            "timeframe",
            "total_rows",
            "complete_rows",
            "incomplete_rows",
            "direction_comparable_rows",
            "direction_matches",
            "direction_mismatches",
            "direction_accuracy_pct",
            "range_comparable_rows",
            "range_matches",
            "range_mismatches",
            "range_accuracy_pct",
        ],
    )
    print_table(
        "Summary By Timeframe + Trigger Minute",
        minute_summary,
        [
            "timeframe",
            "trigger_minute",
            "total_rows",
            "complete_rows",
            "incomplete_rows",
            "direction_comparable_rows",
            "direction_matches",
            "direction_mismatches",
            "direction_accuracy_pct",
            "range_comparable_rows",
            "range_matches",
            "range_mismatches",
            "range_accuracy_pct",
        ],
    )

    preview_rows = rows[: max(args.print_limit, 0)]
    if preview_rows:
        print_table(
            "Detail Preview",
            preview_rows,
            [
                "ts_bucket",
                "timeframe",
                "predicted_trend",
                "truth_trend",
                "direction_is_match",
                "predicted_support",
                "truth_support",
                "predicted_resistance",
                "truth_resistance",
                "range_is_match",
                "complete_window",
                "path_state",
                "close_return_pct",
                "max_up_pct",
                "max_down_pct",
            ],
        )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
