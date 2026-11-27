#!/usr/bin/env python3
"""
Sweep results analyzer: handles baseline / latency / failure experiments.

Directory layout:
  results/<group>/<mode>/<run>/
    *.csv        -- per-node summary (one row per label)
    *.ts.csv     -- per-node 1s timeseries (one row per label per second)
    exp_info.json

Group classification: read fault_type from exp_info.json, fall back to
directory name suffix (-latency / -failure / else baseline).

Outputs:
  results/report.md    -- full markdown report (also echoed to console); per dataset:
                          trimmed throughput tables, a raw-distribution Stability block,
                          and (failure) a recovery-distribution + recovery-winners-by-node
                          table with fastest / 2nd-fastest win share.
  results/plots/*.png  -- <name>_mixed.png, <name>_read_only.png, <name>_timeseries.png,
                          comparison_datasets.png (referenced inline from report.md).

Analysis is CONFIG-DRIVEN: which groups and which run numbers to analyze come
from a JSON file (default: scripts/analyze_sweep.json). To add/remove an
experiment from the analysis, edit the `datasets` list in that JSON — only the
groups listed there are analyzed; deleting a group (or a whole dataset) drops it.

Usage:
  python3 scripts/analyze_sweep.py                       # uses scripts/analyze_sweep.json
  python3 scripts/analyze_sweep.py --config my.json

Config schema (scripts/analyze_sweep.json):
  {
    "results_dir": "results",
    "datasets": [
      {"name": "wan-new", "title": "...", "fault_type": "wan",
       "groups": ["raft-wan", "netelect-jacobson-wan", ...],
       "runs": "33-52"}          # range "a-b", list "1,3,5-8", or [..]
    ]
  }
"""
import csv
import json
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ── Report output (tee: console + markdown buffer) ─────────────────────────────

_REPORT: list[str] = []


def emit(line: str = "") -> None:
    """Append a line to the markdown report AND echo it to the console."""
    _REPORT.append(line)
    print(line)


def emit_table(lines: list[str]) -> None:
    """Emit a fixed-width table wrapped in a fenced code block (monospace in md)."""
    emit("```text")
    for ln in lines:
        emit(ln)
    emit("```")


def write_report(path: Path) -> None:
    path.write_text("\n".join(_REPORT) + "\n")
    print(f"\nReport written to {path}")


def dist_stats(values: list[float]) -> dict | None:
    """Full distribution summary: mean, std (population), min, median, p95, max, n."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    p95 = s[min(n - 1, int(round(0.95 * (n - 1))))]
    return {
        "mean":   statistics.mean(s),
        "std":    statistics.pstdev(s) if n > 1 else 0.0,
        "min":    s[0],
        "median": statistics.median(s),
        "p95":    p95,
        "max":    s[-1],
        "n":      n,
    }

# ── Variant ordering / display ─────────────────────────────────────────────────

VARIANT_ORDER = ["raft", "netelect-raft", "netelect-jacobson"]
VARIANT_SHORT = {
    "raft":              "Raft",
    "netelect-raft":     "NE-Raft",
    "netelect-jacobson": "NE-Jacobson",
}
COLORS = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
FAULT_COLORS = {"baseline": "#4C72B0", "latency": "#DD8452", "failure": "#55A868", "wan": "#C44E52"}

# Longer/more-specific suffixes first so base_variant() strips e.g. "-wan-pde"
# (petition_direct_election arm) before the plain "-wan" check would miss it.
_FAULT_SUFFIXES = {"-wan-pde": "wan", "-latency": "latency", "-failure": "failure", "-wan": "wan"}


def normalize_fault(raw: str) -> str:
    """Map any raw fault_type (e.g. 'wan_tiered_delayed') to a canonical bucket."""
    r = (raw or "").lower()
    if "wan" in r:
        return "wan"
    if "failure" in r or "stepdown" in r:
        return "failure"
    if "latency" in r:
        return "latency"
    if r in ("baseline", "", "none"):
        return "baseline"
    return r


def base_variant(group: str) -> str:
    for suffix in _FAULT_SUFFIXES:
        if group.endswith(suffix):
            return group[: -len(suffix)]
    return group


def _short(variant: str) -> str:
    return VARIANT_SHORT.get(variant, variant)


# ── Group scanning ─────────────────────────────────────────────────────────────

def scan_groups(results_dir: Path) -> dict[str, list[str]]:
    """Return {fault_type: [group, ...]} ordered by VARIANT_ORDER within each type."""
    by_type: dict[str, list[str]] = defaultdict(list)
    for d in results_dir.iterdir():
        if not d.is_dir() or d.name == "plots":
            continue
        ft = next(
            (v for suf, v in _FAULT_SUFFIXES.items() if d.name.endswith(suf)),
            "baseline",
        )
        # Override from exp_info if the field is present (new-format runs).
        infos = list(d.rglob("exp_info.json"))
        if infos:
            try:
                data = json.loads(infos[0].read_text())
                if "fault_type" in data:
                    ft = data["fault_type"]
            except Exception:
                pass
        by_type[normalize_fault(ft)].append(d.name)

    def _sort_key(g):
        v = base_variant(g)
        return VARIANT_ORDER.index(v) if v in VARIANT_ORDER else 99

    return {ft: sorted(gs, key=_sort_key) for ft, gs in by_type.items()}


# ── Statistics helpers ─────────────────────────────────────────────────────────

def trimmed_stats(values: list[float]) -> tuple[float, float]:
    """(mean, stdev) after dropping the single min and max."""
    if len(values) == 1:
        return values[0], 0.0
    if len(values) == 2:
        return statistics.mean(values), statistics.stdev(values)
    trimmed = sorted(values)[1:-1]
    mean  = statistics.mean(trimmed)
    stdev = statistics.stdev(trimmed) if len(trimmed) > 1 else 0.0
    return mean, stdev


# ── Summary CSV loading ────────────────────────────────────────────────────────

def aggregate_run(run_dir: Path, label: str) -> dict | None:
    """Sum/mean across all per-node summary CSVs for one run × label."""
    rows = []
    for f in sorted(run_dir.glob("*.csv")):
        if f.name.endswith(".ts.csv"):
            continue
        try:
            with open(f) as fh:
                for row in csv.DictReader(fh):
                    if row.get("label") == label:
                        rows.append(row)
        except Exception:
            pass
    if not rows:
        return None
    total   = sum(int(r["total"])   for r in rows)
    success = sum(int(r["success"]) for r in rows)
    failed  = sum(int(r["failed"])  for r in rows)
    return {
        "cluster_qps":  sum(float(r["qps"])      for r in rows),
        "p50_ms":       statistics.mean(float(r["p50_ms"])  for r in rows),
        "p95_ms":       statistics.mean(float(r["p95_ms"])  for r in rows),
        "p99_ms":       statistics.mean(float(r["p99_ms"])  for r in rows),
        "p999_ms":      statistics.mean(float(r["p999_ms"]) for r in rows),
        "total":        total,
        "success":      success,
        "failed":       failed,
        "success_rate": 100.0 * success / total if total else 0.0,
        "timeout":      sum(int(r["timeout"])    for r in rows),
        "not_leader":   sum(int(r["not_leader"]) for r in rows),
    }


def select_run_dirs(mode_dir: Path, n_runs: int, run_start: int | None,
                    run_filter: set[int] | None = None) -> list[Path]:
    """Pick run dirs. Precedence: explicit run_filter set > [run_start, +n_runs) window > first n_runs."""
    dirs = sorted(
        [d for d in mode_dir.iterdir() if d.is_dir() and d.name.isdigit()],
        key=lambda d: int(d.name),
    )
    if run_filter is not None:
        return [d for d in dirs if int(d.name) in run_filter]
    if run_start is not None:
        return [d for d in dirs if run_start <= int(d.name) < run_start + n_runs]
    return dirs[:n_runs]


def parse_runs_spec(spec) -> set[int]:
    """Parse a runs spec into a set of run numbers.
    Accepts a list of ints, or a string like "13-32" / "1,3,5-8" / "33-52"."""
    out: set[int] = set()
    if spec is None:
        return out
    if isinstance(spec, (list, tuple)):
        for x in spec:
            out |= parse_runs_spec(x)
        return out
    s = str(spec).strip()
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "-" in tok:
            a, b = tok.split("-", 1)
            out |= set(range(int(a), int(b) + 1))
        else:
            out.add(int(tok))
    return out


def collect_stats(results_dir: Path, groups: list[str], mode: str, label: str,
                  n_runs: int, run_start: int | None = None,
                  run_filter: set[int] | None = None) -> dict[str, dict | None]:
    out: dict[str, dict | None] = {}
    for g in groups:
        mode_dir = results_dir / g / mode
        if not mode_dir.exists():
            out[g] = None
            continue
        run_dirs = select_run_dirs(mode_dir, n_runs, run_start, run_filter)
        per_run = [r for r in (aggregate_run(d, label) for d in run_dirs) if r]
        if not per_run:
            out[g] = None
            continue
        n_used = max(len(per_run) - 2, 1)
        stats: dict = {"n": n_used, "n_raw": len(per_run)}
        for col in per_run[0]:
            vals = [r[col] for r in per_run]
            mean, std = trimmed_stats(vals)          # trimmed (main tables)
            stats[col]          = mean
            stats[f"{col}_std"] = std
            stats[f"{col}_dist"] = dist_stats(vals)  # raw distribution (stability module)
        out[g] = stats
    return out


# ── Timeseries CSV loading ─────────────────────────────────────────────────────

def load_run_ts(run_dir: Path) -> dict[int, dict]:
    """
    Sum all *.ts.csv files (across nodes and read+write labels) for one run.
    Returns {second: {qps, failed, total, error_rate}}.
    """
    per_ts: dict[int, dict] = {}
    for f in run_dir.glob("*.ts.csv"):
        try:
            with open(f) as fh:
                for row in csv.DictReader(fh):
                    t = int(round(float(row["timestamp_s"])))
                    if t not in per_ts:
                        per_ts[t] = {"qps": 0.0, "failed": 0, "total": 0}
                    per_ts[t]["qps"]    += float(row["qps"])
                    per_ts[t]["failed"] += int(row["failed"])
                    per_ts[t]["total"]  += int(row["total"])
        except Exception:
            pass
    for t in per_ts:
        tot = per_ts[t]["total"]
        per_ts[t]["error_rate"] = 100.0 * per_ts[t]["failed"] / tot if tot else 0.0
    return per_ts


def collect_ts(results_dir: Path, group: str, mode: str,
               n_runs: int, run_start: int | None = None,
               run_filter: set[int] | None = None) -> dict[int, dict] | None:
    """
    Average timeseries across runs for one group.
    Returns {second: {qps_mean, error_rate_mean}} or None if no ts data.
    """
    mode_dir = results_dir / group / mode
    if not mode_dir.exists():
        return None
    run_dirs = select_run_dirs(mode_dir, n_runs, run_start, run_filter)

    per_sec: dict[int, list[dict]] = defaultdict(list)
    for rd in run_dirs:
        for t, v in load_run_ts(rd).items():
            per_sec[t].append(v)

    if not per_sec:
        return None

    return {
        t: {
            "qps_mean":        statistics.mean(v["qps"]        for v in vlist),
            "error_rate_mean": statistics.mean(v["error_rate"] for v in vlist),
        }
        for t, vlist in sorted(per_sec.items())
    }


def fault_event_s(results_dir: Path, groups: list[str]) -> float | None:
    """Read fault event time (benchmark-relative seconds) from any exp_info.json."""
    for g in groups:
        for run_path in (results_dir / g).rglob("exp_info.json"):
            try:
                data = json.loads(run_path.read_text())
                if "benchmark_failure_at_s" in data:
                    return float(data["benchmark_failure_at_s"])
                if "delay_start_after_s" in data:
                    # Latency is injected delay_start_after_s into each node's life;
                    # mark that instant directly (e.g. 5s) as the injection line.
                    return float(data["delay_start_after_s"])
            except Exception:
                pass
    return None


# ── Console output ─────────────────────────────────────────────────────────────

_COLS = [
    ("QPS",   "cluster_qps",  "cluster_qps_std",  ",.0f", ",.0f", 20),
    ("p50",   "p50_ms",       "p50_ms_std",       ".2f",  ".2f",  13),
    ("p95",   "p95_ms",       "p95_ms_std",       ".2f",  ".2f",  13),
    ("p99",   "p99_ms",       "p99_ms_std",       ".2f",  ".2f",  13),
    ("p99.9", "p999_ms",      "p999_ms_std",      ".2f",  ".2f",  13),
    ("ok%",   "success_rate", None,               ".2f",  None,    7),
]


def print_table(title: str, label: str, groups: list[str], stats_map: dict) -> None:
    hdr = f"{'Group':<30}  {'n':>3}"
    for name, *_, w in _COLS:
        hdr += f"  {name:^{w}}"
    sep = "-" * 30 + "  " + "-" * 3
    for *_, w in _COLS:
        sep += "  " + "-" * w

    emit(f"\n#### {title} — {label.upper()} (trimmed mean ±std)\n")
    rows = [hdr, sep]
    for g in groups:
        s = stats_map.get(g)
        if s is None:
            rows.append(f"{g:<30}  {'—':>3}")
            continue
        row = f"{g:<30}  {s['n']:>3}"
        for _, mcol, scol, mfmt, sfmt, w in _COLS:
            mean_str = format(s[mcol], mfmt)
            cell = f"{mean_str} ±{format(s[scol], sfmt)}" if scol and sfmt else mean_str
            row += f"  {cell:^{w}}"
        rows.append(row)
    emit_table(rows)


def print_stability(groups: list[str], phases: list[tuple[str, dict]]) -> None:
    """Separate module: raw (untrimmed) distribution of cluster QPS — surfaces the
    run-to-run instability that the trimmed-mean tables deliberately hide."""
    emit("\n#### Stability — cluster QPS, raw/untrimmed distribution\n")
    emit("Trimmed tables above drop the best+worst run; this one keeps every run so "
         "high variance / bad-run tails (e.g. election-storm runs) stay visible. "
         "CV = std/mean (lower = more stable).\n")
    rows = [f"{'Group':<26}{'phase':<12}{'n':>3}{'mean':>11}{'std':>9}{'CV%':>7}{'min':>11}{'median':>11}{'max':>11}"]
    rows.append("-" * len(rows[0]))
    for g in groups:
        for phname, smap in phases:
            s = smap.get(g)
            d = s.get("cluster_qps_dist") if s else None
            if not d:
                continue
            cv = 100.0 * d["std"] / d["mean"] if d["mean"] else 0.0
            rows.append(f"{g:<26}{phname:<12}{d['n']:>3}{d['mean']:>11,.0f}{d['std']:>9,.0f}"
                        f"{cv:>7.1f}{d['min']:>11,.0f}{d['median']:>11,.0f}{d['max']:>11,.0f}")
    emit_table(rows)


# ── Bar chart helpers ──────────────────────────────────────────────────────────

def _qps_bar(ax, groups, stats_map, title):
    shorts = [_short(base_variant(g)) for g in groups]
    vals = [stats_map[g]["cluster_qps"]     if stats_map.get(g) else 0 for g in groups]
    errs = [stats_map[g]["cluster_qps_std"] if stats_map.get(g) else 0 for g in groups]
    bars = ax.bar(shorts, vals, color=COLORS[: len(groups)],
                  yerr=errs, capsize=5, error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax.set_title(title, fontsize=11)
    ax.set_ylabel("QPS")
    ax.tick_params(axis="x", rotation=15)
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(),
                f"{v:,.0f}", ha="center", va="bottom", fontsize=8)


def _lat_bar(ax, groups, stats_map, title):
    import numpy as np
    pcts  = ["p50_ms", "p95_ms", "p99_ms", "p999_ms"]
    plbls = ["p50",    "p95",    "p99",    "p99.9"]
    x = np.arange(len(pcts))
    n = max(len(groups), 1)
    w = 0.8 / n
    for i, g in enumerate(groups):
        s = stats_map.get(g)
        if s is None:
            continue
        offset = (i - n / 2 + 0.5) * w
        means = [s[p]          for p in pcts]
        stds  = [s[f"{p}_std"] for p in pcts]
        ax.bar(x + offset, means, w, label=_short(base_variant(g)),
               color=COLORS[i % len(COLORS)],
               yerr=stds, capsize=3, error_kw={"elinewidth": 1.2, "ecolor": "black"})
    ax.set_title(title, fontsize=11)
    ax.set_ylabel("Latency (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels(plbls)
    ax.legend(fontsize=8)


def _mixed_combined(ax_qps, ax_p99, groups, mixed_read, mixed_write):
    import numpy as np
    shorts = [_short(base_variant(g)) for g in groups]
    r_qps = [mixed_read[g]["cluster_qps"]      if mixed_read.get(g)  else 0 for g in groups]
    w_qps = [mixed_write[g]["cluster_qps"]     if mixed_write.get(g) else 0 for g in groups]
    r_std = [mixed_read[g]["cluster_qps_std"]  if mixed_read.get(g)  else 0 for g in groups]
    w_std = [mixed_write[g]["cluster_qps_std"] if mixed_write.get(g) else 0 for g in groups]
    tot   = [r + w for r, w in zip(r_qps, w_qps)]
    tot_s = [(rs**2 + ws**2) ** 0.5 for rs, ws in zip(r_std, w_std)]

    ax_qps.bar(shorts, w_qps, color=COLORS[1], label="Write")
    ax_qps.bar(shorts, r_qps, bottom=w_qps, color=COLORS[0], label="Read",
               yerr=tot_s, capsize=5, error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax_qps.set_title("Total QPS (Read + Write)", fontsize=11)
    ax_qps.set_ylabel("QPS")
    ax_qps.tick_params(axis="x", rotation=15)
    ax_qps.legend(fontsize=8)
    for i, t in enumerate(tot):
        ax_qps.text(i, t, f"{t:,.0f}", ha="center", va="bottom", fontsize=8)

    x  = np.arange(len(groups))
    bw = 0.35
    r_p99 = [mixed_read[g]["p99_ms"]      if mixed_read.get(g)  else 0 for g in groups]
    w_p99 = [mixed_write[g]["p99_ms"]     if mixed_write.get(g) else 0 for g in groups]
    r_s99 = [mixed_read[g]["p99_ms_std"]  if mixed_read.get(g)  else 0 for g in groups]
    w_s99 = [mixed_write[g]["p99_ms_std"] if mixed_write.get(g) else 0 for g in groups]
    ax_p99.bar(x - bw / 2, r_p99, bw, label="Read p99",  color=COLORS[0],
               yerr=r_s99, capsize=4, error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax_p99.bar(x + bw / 2, w_p99, bw, label="Write p99", color=COLORS[1],
               yerr=w_s99, capsize=4, error_kw={"elinewidth": 1.5, "ecolor": "black"})
    ax_p99.set_title("p99 Latency: Read vs Write", fontsize=11)
    ax_p99.set_ylabel("Latency (ms)")
    ax_p99.set_xticks(x)
    ax_p99.set_xticklabels(shorts, rotation=15)
    ax_p99.legend(fontsize=8)


# ── Bar chart figure builders ──────────────────────────────────────────────────

def make_bar_plots(plots_dir: Path, fault_type: str, groups: list[str],
                   mixed_read: dict, mixed_write: dict, read_only: dict) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  (matplotlib not installed — skipping plots)")
        return

    ft_title = fault_type.capitalize()

    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    fig.suptitle(f"Mixed Workload — {ft_title}", fontsize=14)
    _qps_bar(axes[0, 0], groups, mixed_read,  "Read QPS")
    _qps_bar(axes[0, 1], groups, mixed_write, "Write QPS")
    _lat_bar(axes[1, 0], groups, mixed_read,  "Read Latency")
    _lat_bar(axes[1, 1], groups, mixed_write, "Write Latency")
    _mixed_combined(axes[0, 2], axes[1, 2], groups, mixed_read, mixed_write)
    plt.tight_layout()
    out = plots_dir / f"{fault_type}_mixed.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {out}")

    fig, axes = plt.subplots(1, 2, figsize=(11, 5))
    fig.suptitle(f"Read-Only Workload — {ft_title}", fontsize=14)
    _qps_bar(axes[0], groups, read_only, "Read QPS")
    _lat_bar(axes[1], groups, read_only, "Read Latency")
    plt.tight_layout()
    out = plots_dir / f"{fault_type}_read_only.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {out}")


# ── Timeseries plot ────────────────────────────────────────────────────────────

def make_ts_plot(plots_dir: Path, fault_type: str, groups: list[str],
                 results_dir: Path, n_runs: int, event_s: float | None,
                 run_start: int | None = None, run_filter: set[int] | None = None,
                 recurring: bool = False) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    # Recurring failover (failure) keeps the error-rate panel — its spikes mark
    # the crash instants. Latency-injection faults (wan / wan-pde) show only the
    # cluster-QPS trace with a vertical injection marker.
    show_err = recurring
    if show_err:
        fig, (ax_qps, ax_err) = plt.subplots(1, 2, figsize=(14, 5))
    else:
        fig, ax_qps = plt.subplots(1, 1, figsize=(9, 5))
        ax_err = None
    ft_title = fault_type.capitalize()
    note = " — recurring failover (no single fault instant)" if recurring else ""
    fig.suptitle(
        f"Per-Second Timeseries — {ft_title} (mixed phase, avg over runs){note}", fontsize=13
    )

    has_data = False
    for i, g in enumerate(groups):
        ts = collect_ts(results_dir, g, "mixed", n_runs, run_start, run_filter)
        if not ts:
            continue
        has_data = True
        times  = sorted(ts.keys())
        qps    = [ts[t]["qps_mean"] for t in times]
        lbl    = _short(base_variant(g))
        color  = COLORS[i % len(COLORS)]
        ax_qps.plot(times, qps, color=color, label=lbl, linewidth=1.8)
        if ax_err is not None:
            errate = [ts[t]["error_rate_mean"] for t in times]
            ax_err.plot(times, errate, color=color, label=lbl, linewidth=1.8)

    if not has_data:
        plt.close()
        print(f"  (no timeseries data for {fault_type} — skipping ts plot)")
        return

    if event_s is not None:
        ax_qps.axvline(event_s, color="red", linestyle="--", linewidth=1.2,
                       label=f"latency injection @{event_s:.0f}s")
    ax_qps.legend(fontsize=8)
    ax_qps.set_title("Cluster QPS over time", fontsize=11)
    ax_qps.set_xlabel("Benchmark time (s)")
    ax_qps.set_ylabel("QPS (read + write)")

    if ax_err is not None:
        ax_err.legend(fontsize=8)
        ax_err.set_title("Error rate % over time", fontsize=11)
        ax_err.set_xlabel("Benchmark time (s)")
        ax_err.set_ylabel("Error rate (%)")

    plt.tight_layout()
    out = plots_dir / f"{fault_type}_timeseries.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {out}")


# ── Cross-fault comparison ─────────────────────────────────────────────────────

def make_comparison_plot(plots_dir: Path, groups_by_type: dict[str, list[str]],
                         stats_by_type: dict[str, dict]) -> None:
    """Grouped bar: x=variant, group=fault_type, y=total mixed QPS."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return

    fault_types = [ft for ft in ("baseline", "latency", "wan", "failure") if ft in groups_by_type]
    if not fault_types:
        return

    variant_qps: dict[str, dict[str, float]] = {ft: {} for ft in fault_types}
    for ft in fault_types:
        mr = stats_by_type[ft].get("mixed_read",  {})
        mw = stats_by_type[ft].get("mixed_write", {})
        for g in groups_by_type[ft]:
            v  = base_variant(g)
            r  = mr.get(g) or {}
            w  = mw.get(g) or {}
            variant_qps[ft][v] = r.get("cluster_qps", 0) + w.get("cluster_qps", 0)

    variants = [v for v in VARIANT_ORDER
                if any(v in variant_qps[ft] for ft in fault_types)]
    if not variants:
        return

    x  = np.arange(len(variants))
    bw = 0.8 / len(fault_types)
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle("Cross-Fault QPS Comparison (Mixed: Read + Write)", fontsize=13)

    for i, ft in enumerate(fault_types):
        vals   = [variant_qps[ft].get(v, 0) for v in variants]
        offset = (i - len(fault_types) / 2 + 0.5) * bw
        color  = FAULT_COLORS.get(ft, "#888888")
        bars   = ax.bar(x + offset, vals, bw, label=ft.capitalize(), color=color)
        for b, v in zip(bars, vals):
            if v > 0:
                ax.text(b.get_x() + b.get_width() / 2, b.get_height(),
                        f"{v:,.0f}", ha="center", va="bottom", fontsize=7, rotation=90)

    ax.set_xticks(x)
    ax.set_xticklabels([_short(v) for v in variants], rotation=15)
    ax.set_ylabel("QPS (Read + Write combined)")
    ax.legend(fontsize=9)
    plt.tight_layout()
    out = plots_dir / "comparison_qps.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {out}")


# ── Petition / leadership log analysis (netelect WAN experiments) ───────────────
#
# Parses per-node server logs (raftd-<id>.log) emitted by cmd/raftd:
#   fault_latency_active: node=X outbound_delay=Yms ...   (per-node quality)
#   [leader_elected] node=X term=T                        (every election win)
#   [petition_send]  node=X term=T leader=L proposed=P ...
#   [petition_accept] node=X term=T petitioners=P force_stepdown_leader=L
#   [petition_stepdown] node=X term=T forced_by=F
#
# Note: petition/leader logs carry no wall-clock timestamp, so "petition success
# time" is approximated from the per-second QPS timeseries (the second at which
# throughput rises from the slow far-leader plateau to the fast near-leader one).

_RE_FAULT  = re.compile(r"fault_latency_active: node=(\d+) outbound_delay=(\d+)ms")
_RE_LED    = re.compile(r"\[leader_elected\] node=(\d+) term=(\d+)(?: ts=(\d+))?")
_RE_PSEND  = re.compile(r"\[petition_send\] node=(\d+) term=\d+ leader=(\d+) proposed=(\d+)")
_RE_PACC   = re.compile(r"\[petition_accept\] node=(\d+) term=\d+ petitioners=\d+ force_stepdown_leader=(\d+)")
_RE_PSTEP  = re.compile(r"\[petition_stepdown\] node=(\d+)")
_RE_EMSG   = re.compile(r"election_msgs: node=(\d+) sent=(\d+) recv=(\d+)")
# failure experiment: timestamped election lifecycle (ms since epoch).
_RE_ESTART = re.compile(r"\[election_start\] node=(\d+) term=(\d+) ts=(\d+) detect_ms=([\d.]+)")


def parse_run_logs(run_dir: Path) -> dict | None:
    """Parse all raftd-*.log in one run dir. Returns per-run aggregates or None."""
    logs = sorted(run_dir.glob("raftd-*.log"))
    if not logs:
        return None
    outbound: dict[int, int] = {}
    elected: list[tuple[int, int]] = []     # (term, node)
    led_ts: dict[tuple[int, int], int] = {}   # (node, term) -> leader_elected wall-clock ms
    estart: dict[tuple[int, int], tuple[int, float]] = {}  # (node, term) -> (ts_ms, detect_ms)
    psend_by_node: dict[int, int] = defaultdict(int)
    pacc = 0
    pacc_by_node: dict[int, int] = defaultdict(int)
    kicked: dict[int, int] = defaultdict(int)
    pstep = 0
    emsg_sent = 0      # cumulative RequestVote messages sent (election traffic)
    emsg_recv = 0      # cumulative RequestVote messages received
    for lf in logs:
        try:
            text = lf.read_text(errors="replace")
        except Exception:
            continue
        for m in _RE_FAULT.finditer(text):
            outbound[int(m.group(1))] = int(m.group(2))
        for m in _RE_LED.finditer(text):
            node, term = int(m.group(1)), int(m.group(2))
            elected.append((term, node))
            if m.group(3) is not None:
                led_ts[(node, term)] = int(m.group(3))
        for m in _RE_ESTART.finditer(text):
            estart[(int(m.group(1)), int(m.group(2)))] = (int(m.group(3)), float(m.group(4)))
        for m in _RE_PSEND.finditer(text):
            psend_by_node[int(m.group(1))] += 1
        for m in _RE_PACC.finditer(text):
            pacc += 1
            pacc_by_node[int(m.group(1))] += 1
            kicked[int(m.group(2))] += 1
        pstep += len(_RE_PSTEP.findall(text))
        for m in _RE_EMSG.finditer(text):
            emsg_sent += int(m.group(2))
            emsg_recv += int(m.group(3))

    elected.sort()
    final_leader = elected[-1][1] if elected else None
    elected_count: dict[int, int] = defaultdict(int)
    for _, node in elected:
        elected_count[node] += 1

    # far group = the 2 nodes with the highest outbound delay this run.
    far = set()
    if len(outbound) >= 2:
        far = {n for n, _ in sorted(outbound.items(), key=lambda kv: -kv[1])[:2]}
    migrated = final_leader is not None and final_leader not in far

    # ── failure-recovery events (recurring forced failover) ────────────────────
    # Each elected leader steps down `leader_stepdown_after_s` later, so EVERY
    # election won at a term beyond the initial one is a post-crash recovery.
    # Collect them all (multiple per run). Identified by term alone (no clock
    # comparison); each event's metrics live on its winner's single clock:
    #   detect_ms   = last heartbeat → became candidate
    #   elec_dur    = became candidate → won
    #   recovery_ms = detect_ms + elec_dur = last heartbeat → won leadership
    recoveries = []   # list of dicts, one per recovery (re-election) event
    if elected:
        init_term = elected[0][0]
        for tm, nd in sorted(set(elected)):
            if tm <= init_term:
                continue
            es = estart.get((nd, tm))
            if es is None or (nd, tm) not in led_ts:
                continue
            detect = es[1]
            dur = led_ts[(nd, tm)] - es[0]
            recoveries.append({"node": nd, "term": tm, "detect_ms": detect,
                               "elec_dur_ms": dur, "recovery_ms": detect + dur,
                               "near": nd not in far})

    return {
        "outbound": outbound,
        "leader_elected": len(elected),
        "final_leader": final_leader,
        "elected_count": dict(elected_count),
        "psend_by_node": dict(psend_by_node),
        "psend_total": sum(psend_by_node.values()),
        "pacc": pacc,
        "pacc_by_node": dict(pacc_by_node),
        "kicked": dict(kicked),
        "pstep": pstep,
        "emsg_sent": emsg_sent,
        "emsg_recv": emsg_recv,
        "far": far,
        "migrated": migrated,
        "recoveries": recoveries,
    }


def migration_time_from_ts(run_dir: Path) -> float | None:
    """Approx benchmark-second at which read QPS rises to the migrated plateau."""
    by_sec: dict[int, float] = defaultdict(float)
    for f in run_dir.glob("*.ts.csv"):
        try:
            with open(f) as fh:
                for row in csv.DictReader(fh):
                    if row.get("label") == "read":
                        by_sec[int(round(float(row["timestamp_s"])))] += float(row["qps"])
        except Exception:
            pass
    if len(by_sec) < 3:
        return None
    secs = sorted(by_sec)
    qs = [by_sec[t] for t in secs]
    qmin, qmax = min(qs), max(qs)
    if qmax < 1.4 * max(qmin, 1):          # no clear rise → no observable migration
        return None
    thr = qmin + 0.5 * (qmax - qmin)
    for t in secs:
        if by_sec[t] >= thr:
            return float(t)
    return None


def collect_log_metrics(results_dir: Path, group: str, n_runs: int,
                        run_start: int | None, run_filter: set[int] | None = None) -> dict | None:
    mode_dir = results_dir / group / "mixed"
    if not mode_dir.exists():
        return None
    run_dirs = select_run_dirs(mode_dir, n_runs, run_start, run_filter)
    runs = [r for r in (parse_run_logs(d) for d in run_dirs) if r]
    if not runs:
        return None

    n = len(runs)
    migrated = sum(1 for r in runs if r["migrated"])
    near_petition_runs = 0
    for r in runs:
        if any(node not in r["far"] and cnt > 0 for node, cnt in r["psend_by_node"].items()):
            near_petition_runs += 1

    # Per-node: total elections + mean outbound (quality), over runs.
    node_elect: dict[int, int] = defaultdict(int)
    node_out: dict[int, list[int]] = defaultdict(list)
    final_dist: dict[int, int] = defaultdict(int)
    for r in runs:
        for node, c in r["elected_count"].items():
            node_elect[node] += c
        for node, ob in r["outbound"].items():
            node_out[node].append(ob)
        if r["final_leader"] is not None:
            final_dist[r["final_leader"]] += 1
    # A node with 0 injected delay never logs fault_latency_active, so it is
    # absent from node_out — record it at 0 so it still shows in the table.
    for node in node_elect:
        node_out.setdefault(node, [0])

    mig_times = [t for t in (migration_time_from_ts(d) for d in run_dirs) if t is not None]

    node_out_mean = {k: statistics.mean(v) for k, v in node_out.items()}

    # failure-recovery aggregates: pool every recovery event across all runs.
    events    = [ev for r in runs for ev in r.get("recoveries", [])]
    detects   = [ev["detect_ms"]   for ev in events]
    durations = [ev["elec_dur_ms"] for ev in events]
    recos     = [ev["recovery_ms"] for ev in events]
    near_wins = sum(1 for ev in events if ev["near"])
    n_events  = len(events)

    # Recovery-winner distribution by node (recovery events only — excludes the
    # initial pinned election, unlike node_elect). Identify the fastest and second
    # fastest nodes by injected outbound so we can report their win share directly.
    rec_winners: dict[int, int] = defaultdict(int)
    for ev in events:
        rec_winners[ev["node"]] += 1
    by_speed = sorted(node_out_mean, key=lambda nd: node_out_mean[nd])  # ascending latency
    fastest  = by_speed[0] if by_speed else None
    second   = by_speed[1] if len(by_speed) > 1 else None
    fw = (100.0 * rec_winners.get(fastest, 0) / n_events) if n_events and fastest is not None else None
    sw = (100.0 * rec_winners.get(second,  0) / n_events) if n_events and second  is not None else None

    return {
        "n": n,
        "migration_success_rate": 100.0 * migrated / n,
        "recovery_ms_mean":   statistics.mean(recos)     if recos     else None,
        "recovery_dist":      dist_stats(recos),
        "detect_dist":        dist_stats(detects),
        "recovery_events":    n_events,
        "recoveries_per_run": (n_events / n) if n else 0.0,
        "win_detect_ms_mean": statistics.mean(detects)   if detects   else None,
        "win_elec_dur_mean":  statistics.mean(durations) if durations else None,
        "winner_near_pct":    (100.0 * near_wins / n_events) if n_events else None,
        "rec_winners":        dict(rec_winners),
        "fastest_node":       fastest,
        "second_node":        second,
        "fastest_win_pct":    fw,
        "second_win_pct":     sw,
        "two_fastest_win_pct": (fw + sw) if (fw is not None and sw is not None) else None,
        "node_out_mean":      node_out_mean,
        "avg_leader_elected": statistics.mean(r["leader_elected"] for r in runs),
        "avg_psend": statistics.mean(r["psend_total"] for r in runs),
        "avg_pacc": statistics.mean(r["pacc"] for r in runs),
        "avg_emsg_sent": statistics.mean(r["emsg_sent"] for r in runs),
        "avg_emsg_recv": statistics.mean(r["emsg_recv"] for r in runs),
        "near_petition_run_pct": 100.0 * near_petition_runs / n,
        "node_elect": dict(node_elect),
        "node_out": {k: statistics.mean(v) for k, v in node_out.items()},
        "final_dist": dict(final_dist),
        "mig_time_mean": statistics.mean(mig_times) if mig_times else None,
        "mig_time_n": len(mig_times),
    }


def print_petition_tables(fault_type: str, groups: list[str], results_dir: Path,
                          n_runs: int, run_start: int | None,
                          run_filter: set[int] | None = None) -> None:
    metrics = {g: collect_log_metrics(results_dir, g, n_runs, run_start, run_filter) for g in groups}
    if not any(metrics.values()):
        emit("\n_(no server logs found — skipping petition/leadership analysis)_")
        return

    emit(f"\n### Petition / leadership — fault={fault_type} (from mixed-phase server logs)\n")
    emit("Column meanings:\n")
    emit("- **n** — runs analyzed for this group.")
    emit("- **migrate%** — share of runs whose *final* leader is a near (non-far) node, i.e. "
         "leadership successfully migrated off the slow far leader. 100% = always ended on a fast node.")
    emit("- **mig_t(s)** — approx benchmark-second at which read QPS rises from the slow-far-leader "
         "plateau to the fast-near-leader plateau (read from the per-second timeseries, since "
         "petition logs carry no wall-clock). Lower = migration took effect sooner. `n/a` = no clear rise.")
    emit("- **LE/run** — average `[leader_elected]` events per run = number of leadership changes. "
         "Higher = more churn (extra re-elections / split votes).")
    emit("- **pSend/run** — average `[petition_send]` per run: how many petitions followers sent toward "
         "a proposed faster leader.")
    emit("- **pAcc/run** — average `[petition_accept]` per run: how many petitions a proposed leader "
         "accepted (majority petitioned *and* its RTT was no worse) → it then forces the old leader down.")
    emit("- **nearPetn%** — share of runs in which at least one *near* node sent a petition. Confirms the "
         "fast nodes (not just far ones) actually drive the migration.")
    emit("- **eMsgSent/run · eMsgRecv/run** — average cumulative `Raft.RequestVote` messages sent / received "
         "per run (election control-plane traffic). Lower = fewer/cleaner elections.\n")
    hdr = (f"{'Group':<24}{'n':>3}{'migrate%':>9}{'mig_t(s)':>9}"
           f"{'LE/run':>8}{'pSend/run':>10}{'pAcc/run':>9}{'nearPetn%':>10}"
           f"{'eMsgSent/run':>13}{'eMsgRecv/run':>13}")
    rows = [hdr, "-" * len(hdr)]
    for g in groups:
        m = metrics.get(g)
        if m is None:
            rows.append(f"{g:<24}{'—':>3}")
            continue
        mt = f"{m['mig_time_mean']:.1f}" if m["mig_time_mean"] is not None else "n/a"
        rows.append(f"{g:<24}{m['n']:>3}{m['migration_success_rate']:>8.0f}%{mt:>9}"
                    f"{m['avg_leader_elected']:>8.1f}{m['avg_psend']:>10.0f}"
                    f"{m['avg_pacc']:>9.1f}{m['near_petition_run_pct']:>9.0f}%"
                    f"{m['avg_emsg_sent']:>13.0f}{m['avg_emsg_recv']:>13.0f}")
    emit_table(rows)

    # Per-node election-by-quality table (one block per netelect group).
    for g in groups:
        m = metrics.get(g)
        if m is None or base_variant(g) == "raft":
            continue
        nodes = sorted(m["node_out"], key=lambda nd: m["node_out"][nd])
        total_e = sum(m["node_elect"].values()) or 1
        prows = [f"{'node':<6}{'avg_out(ms)':>12}{'elected':>9}{'share%':>8}{'final_leader×':>14}"]
        for nd in nodes:
            ec = m["node_elect"].get(nd, 0)
            prows.append(f"n{nd:<5}{m['node_out'][nd]:>12.0f}{ec:>9}"
                         f"{100.0*ec/total_e:>7.0f}%{m['final_dist'].get(nd,0):>14}")
        emit(f"\n**[{g}] per-node election by quality (fastest→slowest):**\n")
        emit("_node = node id; avg_out(ms) = injected outbound delay (network quality, lower = faster); "
             "elected = total `[leader_elected]` wins; share% = its fraction of all wins; "
             "final_leader× = runs that ended with this node as leader. Wins should concentrate on the "
             "fast (low-out) nodes._\n")
        emit_table(prows)


def print_failure_tables(fault_type: str, groups: list[str], results_dir: Path,
                         n_runs: int, run_start: int | None,
                         run_filter: set[int] | None = None) -> None:
    """Post-failure recovery + re-election quality (from timestamped server logs)."""
    metrics = {g: collect_log_metrics(results_dir, g, n_runs, run_start, run_filter) for g in groups}
    if not any(metrics.values()):
        emit("\n_(no server logs found — skipping failure-recovery analysis)_")
        return

    emit(f"\n### Failure recovery — fault={fault_type} (leader crash → re-election, mixed-phase logs)\n")
    emit("Recurring failover: each leader steps down ~N s after winning, so every re-election is a "
         "recovery event; metrics pool ALL events across runs (ev = total events). "
         "detect = last heartbeat → candidate; recovery = detect + (candidate → won), winner's single clock. "
         "**Recovery is reported as a distribution (med / p95 / max), not just mean — a single election-storm "
         "run can blow up the mean while the median stays normal.**\n")
    hdr = (f"{'Group':<24}{'ev':>4}{'crash/run':>10}{'winNear%':>9}"
           f"{'rec_med':>9}{'rec_p95':>9}{'rec_max':>9}{'rec_std':>9}"
           f"{'det_med':>9}{'det_max':>9}{'eMsg/run':>9}")
    rows = [hdr, "-" * len(hdr)]
    for g in groups:
        m = metrics.get(g)
        if m is None:
            rows.append(f"{g:<24}{'—':>4}")
            continue
        rd = m["recovery_dist"] or {}
        dd = m["detect_dist"] or {}
        def _g(d, k, p=0): return (f"{d[k]:>9.{p}f}" if d.get(k) is not None else f"{'n/a':>9}")
        wn = f"{m['winner_near_pct']:>8.0f}%" if m['winner_near_pct'] is not None else f"{'n/a':>9}"
        rows.append(f"{g:<24}{m['recovery_events']:>4}{m['recoveries_per_run']:>10.1f}{wn}"
                    f"{_g(rd,'median')}{_g(rd,'p95')}{_g(rd,'max')}{_g(rd,'std')}"
                    f"{_g(dd,'median')}{_g(dd,'max')}{m['avg_emsg_sent']:>9.0f}")
    emit_table(rows)

    # Recovery-winner share by node (recovery events ONLY — excludes the pinned
    # initial election), with the fastest / 2nd-fastest node win rate called out.
    emit("\n#### Recovery winners by node (recovery events only)\n")
    emit("Where leadership lands AFTER a crash (the initial pinned election is excluded). "
         "fastest% / 2nd% = share won by the lowest- and second-lowest-latency node.\n")
    summ = [f"{'Group':<24}{'fastest':>9}{'fast win%':>10}{'2nd':>6}{'2nd win%':>10}{'two-fastest%':>14}"]
    summ.append("-" * len(summ[0]))
    for g in groups:
        m = metrics.get(g)
        if m is None:
            summ.append(f"{g:<24}{'—':>9}")
            continue
        f_, s_ = m["fastest_node"], m["second_node"]
        fl = f"n{f_}({m['node_out_mean'].get(f_,0):.0f}ms)" if f_ is not None else "n/a"
        sl = f"n{s_}" if s_ is not None else "n/a"
        fw = f"{m['fastest_win_pct']:>9.0f}%" if m['fastest_win_pct'] is not None else f"{'n/a':>10}"
        sw = f"{m['second_win_pct']:>9.0f}%" if m['second_win_pct'] is not None else f"{'n/a':>10}"
        tw = f"{m['two_fastest_win_pct']:>13.0f}%" if m['two_fastest_win_pct'] is not None else f"{'n/a':>14}"
        summ.append(f"{g:<24}{fl:>9}{fw}{sl:>6}{sw}{tw}")
    emit_table(summ)

    # Per-node recovery-winner breakdown (one block per group).
    for g in groups:
        m = metrics.get(g)
        if m is None:
            continue
        nodes = sorted(m["node_out_mean"], key=lambda nd: m["node_out_mean"][nd])
        total = sum(m["rec_winners"].values()) or 1
        prows = [f"{'node':<6}{'out(ms)':>9}{'recov_wins':>12}{'win%':>8}{'final×':>8}"]
        for nd in nodes:
            rw = m["rec_winners"].get(nd, 0)
            prows.append(f"n{nd:<5}{m['node_out_mean'][nd]:>9.0f}{rw:>12}"
                         f"{100.0*rw/total:>7.0f}%{m['final_dist'].get(nd,0):>8}")
        emit(f"\n**[{g}] recovery winners by node (fastest→slowest):**\n")
        emit_table(prows)


# ── Entry point ────────────────────────────────────────────────────────────────

def infer_fault_type(results_dir: Path, groups: list[str]) -> str:
    """Best-effort fault category for a set of groups (exp_info first, then name suffix)."""
    for g in groups:
        for p in (results_dir / g).rglob("exp_info.json"):
            try:
                d = json.loads(p.read_text())
                if "fault_type" in d:
                    return normalize_fault(d["fault_type"])
            except Exception:
                pass
    for g in groups:
        for suf, v in _FAULT_SUFFIXES.items():
            if g.endswith(suf):
                return v
    return "baseline"


def analyze_one(label_name: str, fault_type: str, groups: list[str],
                results_dir: Path, plots_dir: Path, n_runs: int,
                run_start: int | None = None, run_filter: set[int] | None = None,
                plot_prefix: str | None = None) -> dict:
    """Full table + plot analysis for one bucket of groups. Returns its stats dict."""
    prefix = plot_prefix or fault_type
    runs_note = ""
    if run_filter:
        rs = sorted(run_filter)
        runs_note = f" — runs {rs[0]}..{rs[-1]} ({len(rs)} selected)"
    emit(f"\n## {label_name}\n")
    emit(f"_fault={fault_type}, {len(groups)} groups{runs_note}_")

    mixed_read  = collect_stats(results_dir, groups, "mixed", "read",  n_runs, run_start, run_filter)
    mixed_write = collect_stats(results_dir, groups, "mixed", "write", n_runs, run_start, run_filter)
    read_only   = collect_stats(results_dir, groups, "read",  "read",  n_runs, run_start, run_filter)

    emit("\n### Throughput & latency (trimmed mean)")
    print_table(f"{label_name} | MIXED", "read",  groups, mixed_read)
    print_table(f"{label_name} | MIXED", "write", groups, mixed_write)
    print_table(f"{label_name} | READ",  "read",  groups, read_only)

    print_stability(groups, [("mixed-read", mixed_read),
                             ("mixed-write", mixed_write),
                             ("read", read_only)])

    if fault_type in ("wan", "latency"):
        print_petition_tables(fault_type, groups, results_dir, n_runs, run_start, run_filter)
    elif fault_type == "failure":
        print_failure_tables(fault_type, groups, results_dir, n_runs, run_start, run_filter)

    print(f"\nGenerating bar charts ({prefix})...")
    make_bar_plots(plots_dir, prefix, groups, mixed_read, mixed_write, read_only)
    emit(f"\n### Plots\n")
    emit(f"![mixed]({plots_dir.name}/{prefix}_mixed.png)")
    emit(f"![read-only]({plots_dir.name}/{prefix}_read_only.png)")

    if fault_type in ("latency", "wan", "failure"):
        # Recurring failover (leader_stepdown_after_s) has no single fault instant —
        # crashes recur every few seconds and the per-run-averaged curve smears the
        # dips, so a single vertical marker would be misleading. Draw none for failure.
        ev = None if fault_type == "failure" else fault_event_s(results_dir, groups)
        make_ts_plot(plots_dir, prefix, groups, results_dir, n_runs, ev, run_start,
                     run_filter, recurring=(fault_type == "failure"))
        emit(f"![timeseries]({plots_dir.name}/{prefix}_timeseries.png)")

    return {"mixed_read": mixed_read, "mixed_write": mixed_write,
            "read_only": read_only, "groups": groups}


def make_bucket_comparison(plots_dir: Path, bucket_order: list[str],
                           stats_by_bucket: dict[str, dict], out_name: str,
                           title: str) -> None:
    """Grouped bar: x=variant, group=bucket (dataset/fault), y=total mixed QPS."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return
    buckets = [b for b in bucket_order if b in stats_by_bucket]
    if len(buckets) < 1:
        return

    variant_qps: dict[str, dict[str, float]] = {b: {} for b in buckets}
    for b in buckets:
        mr = stats_by_bucket[b].get("mixed_read", {})
        mw = stats_by_bucket[b].get("mixed_write", {})
        for g in stats_by_bucket[b].get("groups", []):
            v = base_variant(g)
            r = mr.get(g) or {}
            w = mw.get(g) or {}
            variant_qps[b][v] = r.get("cluster_qps", 0) + w.get("cluster_qps", 0)

    variants = [v for v in VARIANT_ORDER if any(v in variant_qps[b] for b in buckets)]
    if not variants:
        return
    x = np.arange(len(variants))
    bw = 0.8 / len(buckets)
    fig, ax = plt.subplots(figsize=(max(10, 2 * len(variants)), 6))
    fig.suptitle(title, fontsize=13)
    for i, b in enumerate(buckets):
        vals = [variant_qps[b].get(v, 0) for v in variants]
        offset = (i - len(buckets) / 2 + 0.5) * bw
        bars = ax.bar(x + offset, vals, bw, label=b, color=COLORS[i % len(COLORS)])
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        f"{v:,.0f}", ha="center", va="bottom", fontsize=7, rotation=90)
    ax.set_xticks(x)
    ax.set_xticklabels([_short(v) for v in variants], rotation=15)
    ax.set_ylabel("Mixed QPS (read + write)")
    ax.legend(fontsize=9)
    plt.tight_layout()
    out = plots_dir / out_name
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {out}")


def run_config_mode(cfg_path: Path) -> None:
    cfg = json.loads(cfg_path.read_text())
    results_dir = Path(cfg.get("results_dir", "results"))
    if not results_dir.exists():
        print(f"error: {results_dir} not found", file=sys.stderr)
        sys.exit(1)
    plots_dir = results_dir / (cfg.get("plots_subdir") or "plots")
    plots_dir.mkdir(exist_ok=True)

    datasets = cfg.get("datasets", [])
    emit("# Sweep Analysis Report\n")
    emit(f"_Generated {datetime.now():%Y-%m-%d %H:%M} · config `{cfg_path.name}` · "
         f"results `{results_dir}` · {len(datasets)} datasets_\n")
    emit("> Throughput/latency tables use the **trimmed** mean (drop best+worst run). "
         "The **Stability** block under each dataset reports the raw untrimmed distribution "
         "so high-variance / election-storm runs stay visible.")

    stats_by_ds: dict[str, dict] = {}
    order: list[str] = []
    for ds in datasets:
        name   = ds["name"]
        groups = ds["groups"]
        title  = ds.get("title", name)
        rf     = parse_runs_spec(ds["runs"]) if "runs" in ds else None
        ft     = ds.get("fault_type") or infer_fault_type(results_dir, groups)
        ft     = normalize_fault(ft)
        n_runs = len(rf) if rf else int(ds.get("n_runs", 20))
        stats_by_ds[name] = analyze_one(title, ft, groups, results_dir, plots_dir,
                                        n_runs, run_filter=rf, plot_prefix=name)
        order.append(name)

    if len(order) > 1 and cfg.get("comparison", True):
        print("\nGenerating cross-dataset comparison...")
        make_bucket_comparison(plots_dir, order, stats_by_ds,
                               "comparison_datasets.png",
                               "Cross-Dataset QPS Comparison (Mixed: Read + Write)")
        emit(f"\n## Cross-dataset comparison\n")
        emit(f"![comparison]({plots_dir.name}/comparison_datasets.png)")

    report_path = results_dir / (cfg.get("report_file") or "report.md")
    write_report(report_path)
    print("\nDone.")


def main() -> None:
    """Config-driven only. Datasets (groups + run selection) come from a JSON file.
    Usage: python3 scripts/analyze_sweep.py [--config FILE]
           (default config: scripts/analyze_sweep.json next to this script)"""
    args = sys.argv[1:]
    config_path = Path(__file__).resolve().parent / "analyze_sweep.json"
    i = 0
    while i < len(args):
        if args[i] == "--config" and i + 1 < len(args):
            config_path = Path(args[i + 1]); i += 2
        else:
            print(f"error: unknown argument '{args[i]}'.", file=sys.stderr)
            print("usage: analyze_sweep.py [--config FILE]   (analysis is config-driven; "
                  "edit datasets in the JSON to choose groups & runs)", file=sys.stderr)
            sys.exit(1)

    if not config_path.exists():
        print(f"error: config not found: {config_path}", file=sys.stderr)
        print("Create one (see scripts/analyze_sweep.json): a list of datasets, each with "
              "name / groups / runs.", file=sys.stderr)
        sys.exit(1)

    run_config_mode(config_path)


if __name__ == "__main__":
    main()
