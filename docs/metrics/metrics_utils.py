"""Shared helpers for analysing Mycelium benchmark metrics.

The benchmark harness (``cmd/BenchmarkMain.cpp``) writes a CSV described by
ADR-0007 with the columns:

    operation, k, m, shard_size, object_size, failed_nodes, total_nodes,
    crypto_ms, transport_ms, metadata_ms, total_ms, success, storage_overhead

This module centralises loading, validation and a few small plotting helpers so
the notebooks stay focused on analysis rather than boilerplate.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Default location of the CSV produced by BenchmarkMain, relative to this file.
DEFAULT_CSV_PATH = Path(__file__).resolve().parent / "benchmark_results.csv"

# Fallback sample used when no real benchmark run is available, so the notebooks
# can still be executed end-to-end.
SAMPLE_CSV_PATH = Path(__file__).resolve().parent / "sample_results.csv"

# Columns guaranteed by ADR-0007 / MetricsCollector::export_csv.
EXPECTED_COLUMNS = [
    "operation",
    "k",
    "m",
    "shard_size",
    "object_size",
    "failed_nodes",
    "total_nodes",
    "crypto_ms",
    "transport_ms",
    "metadata_ms",
    "total_ms",
    "success",
    "storage_overhead",
]

OPERATIONS = ["PUT", "GET", "REPAIR"]

_INT_COLUMNS = [
    "k",
    "m",
    "shard_size",
    "object_size",
    "failed_nodes",
    "total_nodes",
    "success",
]
_FLOAT_COLUMNS = ["crypto_ms", "transport_ms", "metadata_ms", "total_ms", "storage_overhead"]


def load_metrics(path: str | Path | None = None) -> pd.DataFrame:
    """Load benchmark metrics into a typed, enriched DataFrame.

    Resolution order when ``path`` is None:
    1. ``benchmark_results.csv`` (real benchmark output) if present.
    2. ``sample_results.csv`` (synthetic fallback) otherwise.

    Raises ``FileNotFoundError`` with an actionable message if neither exists.
    """
    csv_path = _resolve_path(path)

    df = pd.read_csv(csv_path)

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"{csv_path} is missing expected columns: {missing}. "
            "Was it produced by the current BenchmarkMain?"
        )

    for col in _INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["operation"] = df["operation"].astype(str).str.upper()

    return _add_derived_columns(df)


def _resolve_path(path: str | Path | None) -> Path:
    if path is not None:
        resolved = Path(path)
        if not resolved.exists():
            raise FileNotFoundError(f"Metrics CSV not found: {resolved}")
        return resolved

    if DEFAULT_CSV_PATH.exists():
        return DEFAULT_CSV_PATH
    if SAMPLE_CSV_PATH.exists():
        return SAMPLE_CSV_PATH

    raise FileNotFoundError(
        f"No metrics CSV found. Expected {DEFAULT_CSV_PATH.name} "
        f"(run the benchmark) or {SAMPLE_CSV_PATH.name} (sample fallback) in "
        f"{DEFAULT_CSV_PATH.parent}."
    )


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add convenience columns used across notebooks."""
    df = df.copy()
    total_shards = df["k"] + df["m"]
    df["code_rate"] = df["k"] / total_shards
    df["config_label"] = df["k"].astype(str) + "+" + df["m"].astype(str)
    df["object_size_kb"] = df["object_size"] / 1024.0
    # failure_ratio is only meaningful where total_nodes > 0.
    df["failure_ratio"] = (
        df["failed_nodes"] / df["total_nodes"].where(df["total_nodes"] > 0)
    )
    return df


def summary_by(
    df: pd.DataFrame,
    group_cols: list[str],
    metric_cols: list[str],
    aggs: tuple[str, ...] = ("mean", "std", "min", "max", "count"),
) -> pd.DataFrame:
    """Return a grouped aggregation table for the requested metrics."""
    return (
        df.groupby(group_cols, dropna=False)[metric_cols]
        .agg(list(aggs))
        .round(3)
    )


def bar(
    df: pd.DataFrame,
    x: str,
    y: str,
    yerr: str | None = None,
    title: str = "",
    ylabel: str | None = None,
    ax: plt.Axes | None = None,
):
    """Simple labelled bar chart from a (already aggregated) DataFrame."""
    ax = ax or plt.gca()
    err = df[yerr] if yerr and yerr in df.columns else None
    ax.bar(df[x].astype(str), df[y], yerr=err, capsize=4)
    ax.set_xlabel(x)
    ax.set_ylabel(ylabel or y)
    ax.set_title(title)
    ax.grid(axis="y", linestyle=":", alpha=0.6)
    return ax


def line_by_group(
    df: pd.DataFrame,
    x: str,
    y: str,
    group: str,
    title: str = "",
    ylabel: str | None = None,
    logx: bool = False,
    ax: plt.Axes | None = None,
):
    """Plot one line per group value of ``group`` (e.g. per operation)."""
    ax = ax or plt.gca()
    for key, sub in df.groupby(group):
        agg = sub.groupby(x)[y].mean().sort_index()
        ax.plot(agg.index, agg.values, marker="o", label=str(key))
    if logx:
        ax.set_xscale("log")
    ax.set_xlabel(x)
    ax.set_ylabel(ylabel or y)
    ax.set_title(title)
    ax.legend(title=group)
    ax.grid(True, linestyle=":", alpha=0.6)
    return ax
