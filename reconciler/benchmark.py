from __future__ import annotations

import random
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from .batch import BatchAggregatorJob, BatchDataStore
from .engine import ReconciliationEngine
from .models import AnomalyType, Event, ReconciliationReport
from .streaming import PartitionedStreamBroker, StreamingConsumer


@dataclass
class ScenarioMetrics:
    scenario_name: str
    total_events: int
    total_windows: int
    flagged_windows: int
    streaming_throughput_eps: float
    reconciliation_latency_ms: float
    mean_discrepancy_before_pct: float
    max_discrepancy_before_pct: float
    mean_discrepancy_after_pct: float
    false_positive_rate_pct: float
    false_negative_rate_pct: float
    correction_accuracy_pct: float
    root_cause_accuracy_pct: float
    cardinality_mode: str
    memory_bytes_per_window: int
    dropped_events: int
    duplicated_events: int
    late_events: int
    time_series_sample_pct: List[float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def generate_dataset(
    num_events: int = 50_000,
    num_windows: int = 50,
    window_size_sec: float = 10.0,
    distinct_user_pool: int = 2_000,
    seed: int = 42,
) -> List[Event]:
    rng = random.Random(seed)
    total_span_sec = num_windows * window_size_sec
    events: List[Event] = []
    for i in range(num_events):
        ev_time = (i / num_events) * total_span_sec + rng.uniform(0.0, 0.001)
        ev_time = min(total_span_sec - 0.001, max(0.0, ev_time))
        uid = f"user-{rng.randint(1, distinct_user_pool)}"
        amount = rng.randint(100, 10_000)
        events.append(
            Event(
                event_id=f"evt-{i:07d}",
                user_id=uid,
                amount_cents=amount,
                event_time_sec=ev_time,
                arrival_time_sec=ev_time + rng.uniform(0.01, 0.5),
                partition_key=uid,
            )
        )
    return events


def run_simulation(
    scenario_name: str,
    num_events: int = 50_000,
    num_windows: int = 50,
    window_size_sec: float = 10.0,
    drop_rate: float = 0.02,
    duplicate_rate: float = 0.02,
    late_arrival_rate: float = 0.01,
    corrupted_window_filter: Optional[Set[int]] = None,
    window_anomaly_mode: Optional[Dict[int, AnomalyType]] = None,
    threshold_pct: float = 0.5,
    auto_correct: bool = True,
    use_hll: bool = False,
    hll_precision: int = 10,
    seed: int = 42,
) -> Tuple[ScenarioMetrics, ReconciliationReport]:
    events = generate_dataset(
        num_events=num_events,
        num_windows=num_windows,
        window_size_sec=window_size_sec,
        seed=seed,
    )

    batch_store = BatchDataStore(":memory:")
    batch_store.append_batch(events)

    broker = PartitionedStreamBroker(
        num_partitions=8,
        window_size_sec=window_size_sec,
        drop_rate=drop_rate,
        duplicate_rate=duplicate_rate,
        late_arrival_rate=late_arrival_rate,
        corrupted_window_Filter=corrupted_window_filter,
        window_anomaly_mode=window_anomaly_mode,
        seed=seed,
    )
    for ev in events:
        broker.publish(ev)

    consumer = StreamingConsumer(
        window_size_sec=window_size_sec,
        allowed_lateness_sec=5.0,
        deduplicate=False,
        use_hll=use_hll,
        hll_precision=hll_precision,
    )
    throughput_eps = consumer.process_stream(broker.consume_all())

    batch_job = BatchAggregatorJob(store=batch_store, window_size_sec=window_size_sec)
    batch_truth = batch_job.run()

    hll_tol = 6.5 if use_hll else 0.0
    engine = ReconciliationEngine(
        threshold_pct=threshold_pct,
        auto_correct=auto_correct,
        window_size_sec=window_size_sec,
        hll_user_tolerance_pct=hll_tol,
    )
    report = engine.reconcile(
        streaming_store=consumer.serving_store,
        batch_truth=batch_truth,
        corrupted_windows_truth=broker.stats.corrupted_windows,
        window_faults_truth=broker.stats.window_faults,
    )
    batch_store.close()

    card_mode = (
        f"HyperLogLog (p={hll_precision}, m={1 << hll_precision})"
        if use_hll
        else "Exact HashSet"
    )
    metrics = ScenarioMetrics(
        scenario_name=scenario_name,
        total_events=num_events,
        total_windows=report.total_windows,
        flagged_windows=report.flagged_windows,
        streaming_throughput_eps=round(throughput_eps, 2),
        reconciliation_latency_ms=report.reconciliation_latency_ms,
        mean_discrepancy_before_pct=report.mean_discrepancy_before_pct,
        max_discrepancy_before_pct=report.max_discrepancy_before_pct,
        mean_discrepancy_after_pct=report.mean_discrepancy_after_pct,
        false_positive_rate_pct=report.false_positive_rate_pct,
        false_negative_rate_pct=report.false_negative_rate_pct,
        correction_accuracy_pct=report.correction_accuracy_pct,
        root_cause_accuracy_pct=report.root_cause_accuracy_pct,
        cardinality_mode=card_mode,
        memory_bytes_per_window=consumer.cardinality_memory_bytes_per_window,
        dropped_events=broker.stats.dropped_events,
        duplicated_events=broker.stats.duplicated_events,
        late_events=broker.stats.late_events,
        time_series_sample_pct=report.time_series_discrepancy_pct[:10],
    )
    return metrics, report


def run_benchmark_suite() -> List[ScenarioMetrics]:
    corrupted_subset = {w for w in range(50) if w % 2 == 1}
    distinct_modes = {
        w: (
            AnomalyType.DUPLICATE_DELIVERY
            if w % 4 == 1
            else AnomalyType.MESSAGE_LOSS_OR_LATE
        )
        for w in corrupted_subset
    }

    scenarios = [
        {
            "scenario_name": "Targeted Window Anomalies + Root-Cause Attribution (25/50 windows fault-injected)",
            "num_events": 50_000,
            "num_windows": 50,
            "drop_rate": 0.04,
            "duplicate_rate": 0.04,
            "late_arrival_rate": 0.02,
            "corrupted_window_filter": corrupted_subset,
            "window_anomaly_mode": distinct_modes,
            "threshold_pct": 0.5,
            "auto_correct": True,
            "use_hll": False,
        },
        {
            "scenario_name": "HyperLogLog O(1) Sketch Mode (1 KB/window vs. 40 KB Exact Set, 50k events)",
            "num_events": 50_000,
            "num_windows": 50,
            "drop_rate": 0.04,
            "duplicate_rate": 0.04,
            "late_arrival_rate": 0.02,
            "corrupted_window_filter": corrupted_subset,
            "window_anomaly_mode": distinct_modes,
            "threshold_pct": 0.5,
            "auto_correct": True,
            "use_hll": True,
            "hll_precision": 10,
        },
        {
            "scenario_name": "High-Throughput Scale Test (100k events across 100 windows)",
            "num_events": 100_000,
            "num_windows": 100,
            "drop_rate": 0.03,
            "duplicate_rate": 0.03,
            "late_arrival_rate": 0.015,
            "corrupted_window_filter": {w for w in range(100) if w % 3 == 0},
            "window_anomaly_mode": {
                w: (
                    AnomalyType.DUPLICATE_DELIVERY
                    if w % 6 == 0
                    else AnomalyType.MESSAGE_LOSS_OR_LATE
                )
                for w in range(100)
                if w % 3 == 0
            },
            "threshold_pct": 0.5,
            "auto_correct": True,
            "use_hll": False,
        },
    ]
    results: List[ScenarioMetrics] = []
    for cfg in scenarios:
        m, _ = run_simulation(**cfg)
        results.append(m)
    return results
