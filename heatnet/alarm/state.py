"""告警并组的状态对象。

组的生命周期单调前进：``OPEN``（接纳成员）→ ``CLOSED``（时间窗收口，成员冻结）
→ ``CLEARED``（全部成员解除且证据完整）。状态只朝一个方向走，值班员看到的每一次
变化都对应审计里的一条事件，不会出现"解除之后又冒出新成员"的倒挂。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 严重度沿用审计三档，权重只用于首因平局裁决。
SEVERITY_WEIGHT = {"notice": 1, "warning": 2, "alarm": 3}

OPEN = "open"
CLOSED = "closed"
CLEARED = "cleared"

CERTAIN = "certain"
PROVISIONAL = "provisional"
PENDING = "pending"

REPORT_WORST = "report_worst"
HOLD = "hold"
UNCLEAR_POLICIES = (REPORT_WORST, HOLD)


@dataclass
class Member:
    """组内一条告警成员；``momentary`` 点告警入组即视为已解除，不阻塞组解除。"""

    alarm_id: str
    channel: str
    severity: str
    first_ts: float
    first_seq: int
    occurrences: int = 1
    cleared: bool = False
    momentary: bool = False


@dataclass
class Group:
    """一个告警组。"""

    group_id: str
    opened_ts: float
    last_active_ts: float
    members: dict = field(default_factory=dict)
    status: str = OPEN
    member_seq: int = 0
    label: "RootCauseLabel | None" = None
    clear_blocked: bool = False
    cleared_ts: float = 0.0


@dataclass(frozen=True)
class RootCauseLabel:
    """首因标注。``member_seq`` 与 ``rule_version`` 标明它是基于哪版成员集、
    哪版因果规则算出来的，审计回放可以原样复算。"""

    root: str | None
    confidence: str
    reason: str
    member_seq: int
    rule_version: str

    def as_dict(self) -> dict:
        return {
            "root": self.root,
            "confidence": self.confidence,
            "reason": self.reason,
            "member_seq": self.member_seq,
            "rule_version": self.rule_version,
        }


@dataclass(frozen=True)
class ShutdownBasis:
    """停堆依据：只指向首因那一组。``actionable=False`` 表示首因挂起，
    停堆逻辑不得据此动作。"""

    group_id: str
    root: str | None
    root_severity: str
    confidence: str
    actionable: bool
    reason: str
    member_seq: int
    rule_version: str
    members_digest: str

    def as_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "root": self.root,
            "root_severity": self.root_severity,
            "confidence": self.confidence,
            "actionable": self.actionable,
            "reason": self.reason,
            "member_seq": self.member_seq,
            "rule_version": self.rule_version,
            "members_digest": self.members_digest,
        }
