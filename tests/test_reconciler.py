from __future__ import annotations

import unittest

from reconciler.batch import BatchAggregatorJob, BatchDataStore
from reconciler.benchmark import generate_dataset, run_simulation
from reconciler.engine import ReconciliationEngine
from reconciler.models import AnomalyType, Event
from reconciler.sketch import AdaptiveWatermarkTracker, HyperLogLog
from reconciler.streaming import PartitionedStreamBroker, StreamingConsumer


class StreamBatchReconcilerTests(unittest.TestCase):
    def test_zero_faults_produces_zero_discrepancy(self) -> None:
        events = generate_dataset(num_events=5_000, num_windows=10, seed=10)
        batch_store = BatchDataStore(":memory:")
        batch_store.append_batch(events)

        broker = PartitionedStreamBroker(
            num_partitions=4,
            drop_rate=0.0,
            duplicate_rate=0.0,
            late_arrival_rate=0.0,
            seed=10,
        )
        for ev in events:
            broker.publish(ev)

        consumer = StreamingConsumer(window_size_sec=10.0)
        consumer.process_stream(broker.consume_all())

        batch_truth = BatchAggregatorJob(batch_store, window_size_sec=10.0).run()
        engine = ReconciliationEngine(threshold_pct=0.1, auto_correct=True)
        report = engine.reconcile(
            consumer.serving_store,
            batch_truth,
            corrupted_windows_truth=set(),
        )
        batch_store.close()

        self.assertEqual(report.flagged_windows, 0)
        self.assertEqual(report.mean_discrepancy_before_pct, 0.0)
        self.assertEqual(report.false_positive_rate_pct, 0.0)
        self.assertEqual(report.false_negative_rate_pct, 0.0)

    def test_dropped_and_duplicate_events_detected_and_auto_corrected(self) -> None:
        corrupted_windows = {1, 3, 5, 7, 9}
        metrics, report = run_simulation(
            scenario_name="Unit Test Targeted Drift",
            num_events=20_000,
            num_windows=10,
            drop_rate=0.05,
            duplicate_rate=0.00,
            late_arrival_rate=0.02,
            corrupted_window_filter=corrupted_windows,
            threshold_pct=0.5,
            auto_correct=True,
            seed=99,
        )
        self.assertEqual(report.flagged_windows, len(corrupted_windows))
        self.assertEqual(report.corrected_windows, len(corrupted_windows))
        self.assertEqual(metrics.false_positive_rate_pct, 0.0)
        self.assertEqual(metrics.false_negative_rate_pct, 0.0)
        self.assertEqual(metrics.correction_accuracy_pct, 100.0)
        self.assertEqual(metrics.mean_discrepancy_after_pct, 0.0)

    def test_hyperloglog_sketch_cardinality_and_memory_bounds(self) -> None:
        hll = HyperLogLog(precision=10)
        exact_users = {f"user-{i}" for i in range(5_000)}
        for u in exact_users:
            hll.add(u)

        est = hll.count()
        rel_err_pct = abs(est - len(exact_users)) / len(exact_users) * 100.0
        self.assertEqual(hll.memory_bytes, 1024)
        self.assertLess(rel_err_pct, 6.0)

    def test_automated_root_cause_anomaly_classification(self) -> None:
        corrupted = {1, 2, 3, 4}
        modes = {
            1: AnomalyType.DUPLICATE_DELIVERY,
            2: AnomalyType.MESSAGE_LOSS_OR_LATE,
            3: AnomalyType.DUPLICATE_DELIVERY,
            4: AnomalyType.MESSAGE_LOSS_OR_LATE,
        }
        metrics, report = run_simulation(
            scenario_name="Root Cause Classification Test",
            num_events=20_000,
            num_windows=6,
            drop_rate=0.05,
            duplicate_rate=0.05,
            late_arrival_rate=0.0,
            corrupted_window_filter=corrupted,
            window_anomaly_mode=modes,
            threshold_pct=0.5,
            auto_correct=True,
            seed=123,
        )
        self.assertEqual(metrics.root_cause_accuracy_pct, 100.0)
        by_wid = {d.window_id: d for d in report.window_discrepancies}
        self.assertEqual(by_wid[1].diagnosed_anomaly, AnomalyType.DUPLICATE_DELIVERY)
        self.assertEqual(by_wid[2].diagnosed_anomaly, AnomalyType.MESSAGE_LOSS_OR_LATE)

    def test_adaptive_watermark_tracker_bounds_lateness(self) -> None:
        tracker = AdaptiveWatermarkTracker(
            min_lateness_sec=1.0, max_lateness_sec=20.0
        )
        for i in range(100):
            lag = 0.5 if i < 98 else 8.0
            tracker.observe(event_time_sec=10.0, arrival_time_sec=10.0 + lag)
        allowed = tracker.allowed_lateness_sec()
        self.assertGreaterEqual(allowed, 8.0)
        self.assertLessEqual(allowed, 20.0)

    def test_late_arriving_events_beyond_watermark_trigger_reconciliation(self) -> None:
        base_events = [
            Event(
                event_id=f"e-{i}",
                user_id=f"u-{i}",
                amount_cents=1000,
                event_time_sec=2.0,
                arrival_time_sec=2.1 if i < 90 else 50.0,
                partition_key=f"u-{i}",
            )
            for i in range(100)
        ]
        batch_store = BatchDataStore(":memory:")
        batch_store.append_batch(base_events)
        batch_truth = BatchAggregatorJob(batch_store, window_size_sec=10.0).run()

        consumer = StreamingConsumer(window_size_sec=10.0, allowed_lateness_sec=5.0)
        consumer.process_stream(base_events)

        self.assertEqual(consumer.dropped_late_events, 10)
        self.assertEqual(consumer.serving_store[0].event_count, 90)
        self.assertEqual(batch_truth[0].event_count, 100)

        engine = ReconciliationEngine(threshold_pct=1.0, auto_correct=True)
        report = engine.reconcile(
            consumer.serving_store,
            batch_truth,
            corrupted_windows_truth={0},
        )
        batch_store.close()

        self.assertEqual(report.flagged_windows, 1)
        self.assertAlmostEqual(report.window_discrepancies[0].count_diff_pct, 10.0)
        self.assertEqual(consumer.serving_store[0].event_count, 100)
        self.assertEqual(consumer.serving_store[0].distinct_users, 100)
        self.assertEqual(report.mean_discrepancy_after_pct, 0.0)

    def test_reconciliation_without_auto_correction_preserves_streaming_state(self) -> None:
        metrics, report = run_simulation(
            scenario_name="No Auto-Correct Mode",
            num_events=10_000,
            num_windows=10,
            drop_rate=0.05,
            duplicate_rate=0.0,
            late_arrival_rate=0.0,
            threshold_pct=0.5,
            auto_correct=False,
            seed=7,
        )
        self.assertGreater(report.flagged_windows, 0)
        self.assertEqual(report.corrected_windows, 0)
        self.assertEqual(
            report.mean_discrepancy_before_pct, report.mean_discrepancy_after_pct
        )


if __name__ == "__main__":
    unittest.main()
