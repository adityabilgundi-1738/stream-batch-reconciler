from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from .sketch import HyperLogLog


class AnomalyType(str, Enum):
    NONE = "NONE"
    DUPLICATE_DELIVERY = "DUPLICATE_DELIVERY"
    MESSAGE_LOSS_OR_LATE = "MESSAGE_LOSS_OR_LATE"
    MIXED_DRIFT = "MIXED_DRIFT"


@dataclass(frozen=True)
class Event:
    event_id: str
    user_id: str
    amount_cents: int
    event_time_sec: float
    arrival_time_sec: float
    partition_key: str


@dataclass
class WindowAggregate:
    window_id: int
    window_start_sec: float
    window_end_sec: float
    event_count: int = 0
    amount_sum_cents: int = 0
    user_ids: Set[str] = field(default_factory=set)
    hll: Optional[HyperLogLog] = None
    exact_distinct_override: Optional[int] = None

    @property
    def distinct_users(self) -> int:
        if self.exact_distinct_override is not None:
            return self.exact_distinct_override
        if self.hll is not None:
            return self.hll.count()
        return len(self.user_ids)

    def clone_summary(self) -> Dict[str, int]:
        return {
            "window_id": self.window_id,
            "event_count": self.event_count,
            "amount_sum_cents": self.amount_sum_cents,
            "distinct_users": self.distinct_users,
        }


@dataclass
class WindowDiscrepancy:
    window_id: int
    count_diff_pct: float
    sum_diff_pct: float
    distinct_users_diff_pct: float
    max_diff_pct: float
    flagged: bool
    ground_truth_corrupted: bool
    diagnosed_anomaly: AnomalyType
    expected_anomaly: AnomalyType
    auto_corrected: bool
    post_correction_diff_pct: float
    streaming_before: Dict[str, int]
    batch_truth: Dict[str, int]
    streaming_after: Dict[str, int]
    reasons: List[str] = field(default_factory=list)


@dataclass
class ReconciliationReport:
    total_windows: int
    flagged_windows: int
    corrected_windows: int
    reconciliation_latency_ms: float
    mean_discrepancy_before_pct: float
    max_discrepancy_before_pct: float
    mean_discrepancy_after_pct: float
    correction_accuracy_pct: float
    root_cause_accuracy_pct: float
    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int
    false_positive_rate_pct: float
    false_negative_rate_pct: float
    window_discrepancies: List[WindowDiscrepancy] = field(default_factory=list)
    time_series_discrepancy_pct: List[float] = field(default_factory=list)
