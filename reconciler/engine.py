from __future__ import annotations

import statistics
import time
from typing import Dict, List, Optional, Set

from .models import (
    AnomalyType,
    ReconciliationReport,
    WindowAggregate,
    WindowDiscrepancy,
)
from .streaming import WindowFaultCounter


def _pct_diff(streaming_val: int, batch_val: int) -> float:
    if batch_val == 0:
        return 0.0 if streaming_val == 0 else 100.0
    return abs(streaming_val - batch_val) / abs(batch_val) * 100.0


class ReconciliationEngine:
    def __init__(
        self,
        threshold_pct: float = 0.5,
        auto_correct: bool = True,
        window_size_sec: float = 10.0,
        hll_user_tolerance_pct: float = 0.0,
    ) -> None:
        self.threshold_pct = threshold_pct
        self.auto_correct = auto_correct
        self.window_size_sec = window_size_sec
        self.hll_user_tolerance_pct = hll_user_tolerance_pct

    @staticmethod
    def classify_anomaly(
        s_agg: WindowAggregate,
        b_agg: WindowAggregate,
        flagged: bool,
        user_tolerance_pct: float = 0.1,
    ) -> AnomalyType:
        if not flagged:
            return AnomalyType.NONE
        u_diff = _pct_diff(s_agg.distinct_users, b_agg.distinct_users)
        if s_agg.event_count > b_agg.event_count and u_diff <= max(
            0.05, user_tolerance_pct
        ):
            return AnomalyType.DUPLICATE_DELIVERY
        if (
            s_agg.event_count < b_agg.event_count
            and s_agg.amount_sum_cents < b_agg.amount_sum_cents
        ):
            return AnomalyType.MESSAGE_LOSS_OR_LATE
        return AnomalyType.MIXED_DRIFT

    def reconcile(
        self,
        streaming_store: Dict[int, WindowAggregate],
        batch_truth: Dict[int, WindowAggregate],
        corrupted_windows_truth: Optional[Set[int]] = None,
        window_faults_truth: Optional[Dict[int, WindowFaultCounter]] = None,
    ) -> ReconciliationReport:
        batch_done_ns = time.perf_counter_ns()
        all_window_ids = sorted(set(streaming_store.keys()) | set(batch_truth.keys()))

        discrepancies: List[WindowDiscrepancy] = []
        time_series_before: List[float] = []
        diffs_after: List[float] = []

        tp = fp = tn = fn = 0
        flagged_count = 0
        corrected_count = 0
        accurate_corrections = 0
        accurate_diagnoses = 0
        diagnosable_windows = 0

        for wid in all_window_ids:
            w_start = wid * self.window_size_sec
            w_end = w_start + self.window_size_sec
            s_agg = streaming_store.get(
                wid,
                WindowAggregate(
                    window_id=wid, window_start_sec=w_start, window_end_sec=w_end
                ),
            )
            b_agg = batch_truth.get(
                wid,
                WindowAggregate(
                    window_id=wid, window_start_sec=w_start, window_end_sec=w_end
                ),
            )

            before_snapshot = s_agg.clone_summary()
            batch_snapshot = b_agg.clone_summary()

            c_diff = _pct_diff(s_agg.event_count, b_agg.event_count)
            s_diff = _pct_diff(s_agg.amount_sum_cents, b_agg.amount_sum_cents)
            u_diff = _pct_diff(s_agg.distinct_users, b_agg.distinct_users)

            effective_u_diff = (
                0.0 if u_diff <= self.hll_user_tolerance_pct else u_diff
            )
            max_diff = max(c_diff, s_diff, effective_u_diff)
            time_series_before.append(round(max_diff, 4))

            reasons: List[str] = []
            if c_diff > self.threshold_pct:
                reasons.append(
                    f"count drift {c_diff:.2f}% (stream={s_agg.event_count}, batch={b_agg.event_count})"
                )
            if s_diff > self.threshold_pct:
                reasons.append(
                    f"sum drift {s_diff:.2f}% (stream={s_agg.amount_sum_cents}, batch={b_agg.amount_sum_cents})"
                )
            if effective_u_diff > self.threshold_pct:
                reasons.append(
                    f"distinct_users drift {u_diff:.2f}% (stream={s_agg.distinct_users}, batch={b_agg.distinct_users})"
                )

            flagged = max_diff > self.threshold_pct
            if flagged:
                flagged_count += 1

            if corrupted_windows_truth is not None:
                is_actually_corrupted = (
                    wid in corrupted_windows_truth and max_diff > 0.0
                )
            else:
                is_actually_corrupted = max_diff > self.threshold_pct

            if flagged and is_actually_corrupted:
                tp += 1
            elif flagged and not is_actually_corrupted:
                fp += 1
            elif not flagged and is_actually_corrupted:
                fn += 1
            else:
                tn += 1

            diagnosed = self.classify_anomaly(
                s_agg,
                b_agg,
                flagged=flagged,
                user_tolerance_pct=self.hll_user_tolerance_pct,
            )
            expected_anom = AnomalyType.NONE
            if window_faults_truth is not None and wid in window_faults_truth:
                expected_anom = window_faults_truth[wid].expected_anomaly

            if flagged and window_faults_truth is not None:
                diagnosable_windows += 1
                if diagnosed == expected_anom:
                    accurate_diagnoses += 1

            auto_corrected = False
            if flagged and self.auto_correct:
                streaming_store[wid] = WindowAggregate(
                    window_id=wid,
                    window_start_sec=b_agg.window_start_sec,
                    window_end_sec=b_agg.window_end_sec,
                    event_count=b_agg.event_count,
                    amount_sum_cents=b_agg.amount_sum_cents,
                    user_ids=set(b_agg.user_ids),
                    exact_distinct_override=b_agg.distinct_users,
                )
                auto_corrected = True
                corrected_count += 1

            updated_s = streaming_store.get(wid, s_agg)
            after_snapshot = updated_s.clone_summary()
            post_diff = max(
                _pct_diff(updated_s.event_count, b_agg.event_count),
                _pct_diff(updated_s.amount_sum_cents, b_agg.amount_sum_cents),
                _pct_diff(updated_s.distinct_users, b_agg.distinct_users),
            )
            diffs_after.append(post_diff)

            if flagged and post_diff == 0.0:
                accurate_corrections += 1

            discrepancies.append(
                WindowDiscrepancy(
                    window_id=wid,
                    count_diff_pct=round(c_diff, 4),
                    sum_diff_pct=round(s_diff, 4),
                    distinct_users_diff_pct=round(u_diff, 4),
                    max_diff_pct=round(max_diff, 4),
                    flagged=flagged,
                    ground_truth_corrupted=is_actually_corrupted,
                    diagnosed_anomaly=diagnosed,
                    expected_anomaly=expected_anom,
                    auto_corrected=auto_corrected,
                    post_correction_diff_pct=round(post_diff, 4),
                    streaming_before=before_snapshot,
                    batch_truth=batch_snapshot,
                    streaming_after=after_snapshot,
                    reasons=reasons,
                )
            )

        report_done_ns = time.perf_counter_ns()
        reconciliation_latency_ms = (report_done_ns - batch_done_ns) / 1_000_000.0

        fpr = (fp / (fp + tn) * 100.0) if (fp + tn) > 0 else 0.0
        fnr = (fn / (fn + tp) * 100.0) if (fn + tp) > 0 else 0.0
        correction_acc = (
            (accurate_corrections / flagged_count * 100.0)
            if flagged_count > 0
            else 100.0
        )
        root_cause_acc = (
            (accurate_diagnoses / diagnosable_windows * 100.0)
            if diagnosable_windows > 0
            else 100.0
        )

        return ReconciliationReport(
            total_windows=len(all_window_ids),
            flagged_windows=flagged_count,
            corrected_windows=corrected_count,
            reconciliation_latency_ms=round(reconciliation_latency_ms, 4),
            mean_discrepancy_before_pct=round(
                statistics.mean(time_series_before) if time_series_before else 0.0, 4
            ),
            max_discrepancy_before_pct=round(
                max(time_series_before) if time_series_before else 0.0, 4
            ),
            mean_discrepancy_after_pct=round(
                statistics.mean(diffs_after) if diffs_after else 0.0, 4
            ),
            correction_accuracy_pct=round(correction_acc, 2),
            root_cause_accuracy_pct=round(root_cause_acc, 2),
            true_positives=tp,
            false_positives=fp,
            true_negatives=tn,
            false_negatives=fn,
            false_positive_rate_pct=round(fpr, 4),
            false_negative_rate_pct=round(fnr, 4),
            window_discrepancies=discrepancies,
            time_series_discrepancy_pct=time_series_before,
        )
