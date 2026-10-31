from __future__ import annotations

import argparse
import json
from typing import List

from reconciler.benchmark import ScenarioMetrics, run_benchmark_suite, run_simulation


def _print_report(results: List[ScenarioMetrics]) -> None:
    print("=" * 114)
    print("STREAMING VS. BATCH RECONCILIATION ENGINE (LAMBDA ARCHITECTURE) — METRICS REPORT")
    print("=" * 114)
    for m in results:
        print(f"\nScenario: {m.scenario_name}")
        print("-" * 114)
        print(
            f"  Dataset & Windows        : {m.total_events:,} events | "
            f"{m.total_windows} windows | {m.flagged_windows} flagged for drift"
        )
        print(
            f"  Cardinality & Memory     : {m.cardinality_mode} (~{m.memory_bytes_per_window:,} bytes/window)"
        )
        print(
            f"  Injected Stream Faults   : {m.dropped_events:,} dropped | "
            f"{m.duplicated_events:,} duplicates | {m.late_events:,} late past watermark"
        )
        print(f"  Streaming Throughput     : {m.streaming_throughput_eps:,.2f} events/sec")
        print(f"  Reconciliation Latency   : {m.reconciliation_latency_ms:.4f} ms")
        print(
            f"  Discrepancy (Pre-Fix)    : mean = {m.mean_discrepancy_before_pct:.4f}% | "
            f"max = {m.max_discrepancy_before_pct:.4f}%"
        )
        print(
            f"  Detection & Attribution  : FPR = {m.false_positive_rate_pct:.2f}% | "
            f"FNR = {m.false_negative_rate_pct:.2f}% | "
            f"Root-Cause Attribution Accuracy = {m.root_cause_accuracy_pct:.2f}%"
        )
        print(
            f"  Post-Correction State    : mean discrepancy = {m.mean_discrepancy_after_pct:.4f}% | "
            f"Correction Accuracy = {m.correction_accuracy_pct:.2f}%"
        )
        print(f"  Window Drift Sample (%)  : {m.time_series_sample_pct}")
    print("\n" + "=" * 114)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Streaming vs. Batch Reconciliation Engine & Benchmark Runner"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=0,
        help="Run a custom simulation with N events (0 runs the full benchmark suite)",
    )
    parser.add_argument(
        "--drop-rate",
        type=float,
        default=0.03,
        help="Simulated packet/message drop rate on streaming path",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.02,
        help="Simulated duplicate event delivery rate on streaming path",
    )
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=0.5,
        help="Percentage discrepancy threshold for flagging a window",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default="",
        help="Optional path to export benchmark results as JSON",
    )
    args = parser.parse_args()

    if args.events > 0:
        m, _ = run_simulation(
            scenario_name=f"Custom Simulation ({args.events:,} events)",
            num_events=args.events,
            drop_rate=args.drop_rate,
            duplicate_rate=args.duplicate_rate,
            threshold_pct=args.threshold_pct,
        )
        results = [m]
    else:
        results = run_benchmark_suite()

    _print_report(results)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in results], f, indent=2)


if __name__ == "__main__":
    main()
