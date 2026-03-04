#!/usr/bin/env python3
"""
Generate benchmark charts for the slabbis performance report.

All data is from real benchmark runs on Apple M1 using runbenchmark.sh.
Two runs are recorded:
  - v0.1.1  post-MGET-fix (before pool/GetInto work): the baseline
  - v0.1.2  lighter load run: the cleanest representative numbers

Output: PDF files in charts/ suitable for LaTeX includegraphics.
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

OUT = os.path.join(os.path.dirname(__file__), "charts")
os.makedirs(OUT, exist_ok=True)

# ── colour palette ────────────────────────────────────────────────────────────
# Three targets: in-process, slabbis-RESP, Redis.
# Chosen to be distinguishable in greyscale print as well as colour.
C_INPROC  = "#1a6faf"   # blue
C_SLABBIS = "#e07b00"   # orange
C_REDIS   = "#7a5195"   # muted purple

HATCH_INPROC  = ""
HATCH_SLABBIS = ""
HATCH_REDIS   = ""

plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        9,
    "axes.titlesize":   10,
    "axes.labelsize":   9,
    "xtick.labelsize":  8,
    "ytick.labelsize":  8,
    "legend.fontsize":  8,
    "figure.dpi":       150,
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "axes.grid":        True,
    "grid.alpha":       0.3,
    "grid.linestyle":   "--",
})

# ── benchmark data ────────────────────────────────────────────────────────────

OPS = ["GET", "SET", "DEL", "EXISTS", "Mixed\n80/20", "MGET", "MSET"]
OPS_FLAT = ["GET", "SET", "DEL", "EXISTS", "Mixed 80/20", "MGET", "MSET"]

# v0.1.1 (post-MGET-fix, before pool work) — M1, moderate load
# ops/s
V011 = {
    "inproc":  [14.58e6, 7.46e6, 14.01e6, 17.02e6, 6.72e6,  804.5e3, 301.0e3],
    "slabbis": [225.6e3, 202.7e3,184.2e3, 190.0e3, 181.8e3,  68.4e3,  98.2e3],
    "redis":   [203.4e3, 185.7e3,206.8e3, 187.8e3, 182.1e3, 117.9e3, 109.7e3],
}
# p999 µs
V011_P999 = {
    "inproc":  [8.0,   55.9,  114.1, 11.3,  72.5,  1700,  1520],
    "slabbis": [827.8, 730.7, 721.8, 914.8, 922.7, 4690,  3960],
    "redis":   [270.9, 294.2, 376.7, 754.7, 401.3, 1990,  1980],
}

# v0.1.2 — M1, lighter load (cleanest numbers)
# ops/s
V012 = {
    "inproc":  [15.72e6, 7.04e6, 13.91e6, 17.09e6, 7.10e6,  1.86e6,  688.3e3],
    "slabbis": [209.1e3, 196.3e3,208.5e3, 196.3e3, 178.0e3, 162.3e3, 136.7e3],
    "redis":   [195.3e3, 190.3e3,204.0e3, 192.7e3, 185.0e3, 145.7e3, 112.7e3],
}
# p50 µs
V012_P50 = {
    "inproc":  [0.291, 0.375, 0.208, 0.291, 0.250, 2.6,   6.6],
    "slabbis": [37.8,  40.8,  37.5,  41.2,  49.3,  42.3,  51.7],
    "redis":   [43.4,  46.6,  41.7,  44.5,  46.4,  63.2,  82.7],
}
# p99 µs
V012_P99 = {
    "inproc":  [1.1,   26.9,  2.6,   0.834, 23.0,  10.4,  87.0],
    "slabbis": [175.8, 153.0, 152.0, 169.0, 162.4, 230.8, 268.2],
    "redis":   [145.2, 139.7, 136.6, 140.1, 149.5, 173.4, 202.0],
}
# p999 µs
V012_P999 = {
    "inproc":  [3.5,   58.8,  112.7, 5.7,   67.2,  64.0,  192.0],
    "slabbis": [837.6, 723.7, 626.4, 804.6, 728.3, 1500,  2150],
    "redis":   [380.6, 283.8, 383.8, 335.8, 361.9, 271.7, 344.2],
}

# ── helpers ───────────────────────────────────────────────────────────────────

def save(fig, name):
    path = os.path.join(OUT, name)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {path}")

def bar_group(ax, data_list, labels, colors, hatches, op_labels,
              width=0.22, log=False):
    """Draw grouped bars. data_list: list of arrays, one per group member."""
    n_ops = len(op_labels)
    n_grp = len(data_list)
    x = np.arange(n_ops)
    offsets = np.linspace(-(n_grp-1)/2, (n_grp-1)/2, n_grp) * width
    for i, (data, label, color, hatch) in enumerate(
            zip(data_list, labels, colors, hatches)):
        ax.bar(x + offsets[i], data, width,
               label=label, color=color, hatch=hatch,
               edgecolor="white", linewidth=0.5, zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(op_labels)
    if log:
        ax.set_yscale("log")
    ax.legend(framealpha=0.9)
    return ax

def fmt_ops(v):
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    if v >= 1e3:
        return f"{v/1e3:.0f}k"
    return f"{v:.0f}"

# ── Chart 1: Throughput — all targets, v0.1.2, log scale ─────────────────────

def chart_throughput_all():
    fig, ax = plt.subplots(figsize=(7.2, 3.6))

    bar_group(ax,
        [V012["inproc"], V012["slabbis"], V012["redis"]],
        ["In-process", "slabbis-RESP", "Redis"],
        [C_INPROC, C_SLABBIS, C_REDIS],
        [HATCH_INPROC, HATCH_SLABBIS, HATCH_REDIS],
        OPS, log=True)

    ax.set_ylabel("Operations / second (log scale)")
    ax.set_title("Throughput by operation and target — slabbis v0.1.2 (Apple M1)")
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    save(fig, "throughput_all.pdf")

# ── Chart 2: RESP-level — slabbis vs Redis only, linear ──────────────────────

def chart_resp_comparison():
    fig, ax = plt.subplots(figsize=(7.2, 3.4))

    bar_group(ax,
        [V012["slabbis"], V012["redis"]],
        ["slabbis-RESP", "Redis"],
        [C_SLABBIS, C_REDIS],
        [HATCH_SLABBIS, HATCH_REDIS],
        OPS)

    ax.set_ylabel("Operations / second")
    ax.set_title("RESP-level throughput: slabbis vs Redis — v0.1.2 (Apple M1)")
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: fmt_ops(v)))

    save(fig, "resp_comparison.pdf")

# ── Chart 3: Before / after MGET and Mixed 80/20 — key improvements ──────────

def chart_before_after():
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.4))

    # Left: MGET throughput v0.1.1 vs v0.1.2
    # Colours here encode version (before/after), not target identity.
    # Target identity is carried by the x-axis labels.
    ax = axes[0]
    targets = ["In-process", "slabbis-RESP", "Redis"]
    v011_mget = [V011["inproc"][5], V011["slabbis"][5], V011["redis"][5]]
    v012_mget = [V012["inproc"][5], V012["slabbis"][5], V012["redis"][5]]
    x = np.arange(3)
    w = 0.32
    C_BEFORE = "#c9a227"   # ochre — before (shared across both panels)
    C_AFTER  = "#2a9d8f"   # teal — after (shared across both panels, not a target colour)
    ax.bar(x - w/2, v011_mget, w, label="v0.1.1", color=C_BEFORE,
           edgecolor="white", zorder=3)
    ax.bar(x + w/2, v012_mget, w, label="v0.1.2", color=C_AFTER,
           edgecolor="white", zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(targets, fontsize=8)
    ax.set_ylabel("ops / second")
    ax.set_title("MGET throughput: v0.1.1 → v0.1.2")
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: fmt_ops(v)))
    ax.legend(framealpha=0.9)
    ax.set_yscale("log")

    # Right: p999 latency for RESP targets across all ops
    ax = axes[1]
    ops_labels = ["GET", "SET", "DEL", "EXISTS", "Mix", "MGET", "MSET"]
    x = np.arange(len(ops_labels))
    w = 0.32

    ax.bar(x - w/2, V011_P999["slabbis"], w,
           label="slabbis v0.1.1", color=C_BEFORE,
           edgecolor="white", zorder=3)
    ax.bar(x + w/2, V012_P999["slabbis"], w,
           label="slabbis v0.1.2", color=C_AFTER,
           edgecolor="white", zorder=3)

    ax.set_xticks(x)
    ax.set_xticklabels(ops_labels, fontsize=8)
    ax.set_ylabel("p999 latency (µs)")
    ax.set_title("slabbis-RESP p999 latency: v0.1.1 → v0.1.2")
    ax.legend(framealpha=0.9)

    fig.tight_layout(pad=1.2)
    save(fig, "before_after.pdf")

# ── Chart 4: Latency profile — p50 / p99 / p999 for RESP targets ─────────────

def chart_latency_profile():
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.6), sharey=False)

    x = np.arange(len(OPS))
    w = 0.28

    for ax, target, color, title in [
        (axes[0], "slabbis", C_SLABBIS, "slabbis-RESP latency"),
        (axes[1], "redis",   C_REDIS,   "Redis latency"),
    ]:
        p50  = V012_P50[target]
        p99  = V012_P99[target]
        p999 = V012_P999[target]

        ax.bar(x - w,   p50,  w, label="p50",  color=color, alpha=0.5,
               edgecolor="white", zorder=3)
        ax.bar(x,       p99,  w, label="p99",  color=color, alpha=0.75,
               edgecolor="white", zorder=3)
        ax.bar(x + w,   p999, w, label="p999", color=color, alpha=1.0,
               edgecolor="white", zorder=3)

        ax.set_xticks(x)
        ax.set_xticklabels(OPS, fontsize=7.5)
        ax.set_ylabel("Latency (µs)")
        ax.set_title(title)
        ax.legend(framealpha=0.9)

    fig.suptitle("Latency percentiles by operation — v0.1.2 (Apple M1)",
                 fontsize=10, y=1.01)
    fig.tight_layout(pad=1.2)
    save(fig, "latency_profile.pdf")

# ── Chart 5: in-process speedup ratio over Redis ──────────────────────────────

def chart_speedup():
    fig, ax = plt.subplots(figsize=(5.5, 3.2))

    ops_labels = OPS_FLAT
    ratios = [ip / rd for ip, rd in zip(V012["inproc"], V012["redis"])]

    colors = [C_INPROC] * len(ratios)
    bars = ax.barh(ops_labels, ratios, color=colors,
                   edgecolor="white", zorder=3)

    for bar, ratio in zip(bars, ratios):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                f"{ratio:.0f}×", va="center", fontsize=8)

    ax.set_xlabel("Speed-up over Redis (same operation, in-process vs TCP+RESP)")
    ax.set_title("In-process advantage — v0.1.2 (Apple M1)")
    ax.axvline(1, color="#999999", linewidth=0.8, linestyle="--")
    ax.set_xlim(0, max(ratios) * 1.15)

    fig.tight_layout()
    save(fig, "speedup.pdf")

# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating charts...")
    chart_throughput_all()
    chart_resp_comparison()
    chart_before_after()
    chart_latency_profile()
    chart_speedup()
    print("Done.")
