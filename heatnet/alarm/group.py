"""告警观测、告警组与状态常量。

一条现场上报落成一条 :class:`AlarmObservation`（带序号与发生时间，``cleared=False``
表示告警发生，``cleared=True`` 表示该通道恢复正常）。同一因果链上、落在因果窗内
的观测归进同一个 :class:`AlarmGroup`。

组的生命周期被钉死为单向推进，时序不能乱：

    open（开窗接收） → closed（闭窗封组） → recovered（整组确认解除）

首因标注独立成一个字段（``root_status``）：

    pending（首因不明，挂起） → confirmed（上游首因按时到达）
                             → inferred（挂起到点仍不明，按最重一条推定）

组在 ``closed`` 之后还能补录迟到的因果告警，但首因标注不会回退；已经 ``recovered``
的组只接受迟到的恢复帧，不接受新告警（新告警另开新组）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 组状态。
GROUP_OPEN = "open"
GROUP_CLOSED = "closed"
GROUP_RECOVERED = "recovered"
GROUP_STATES = (GROUP_OPEN, GROUP_CLOSED, GROUP_RECOVERED)

# 首因标注状态。
ROOT_PENDING = "pending"
ROOT_CONFIRMED = "confirmed"
ROOT_INFERRED = "inferred"
ROOT_STATUS = (ROOT_PENDING, ROOT_CONFIRMED, ROOT_INFERRED)

# 告警组的并组依据，落审计、供控制台展示，口径只有一个。
GROUPING_POLICY = "causal-chain"


@dataclass
class AlarmObservation:
    """一路告警的一次发生或恢复。"""

    seq: int
    channel: str
    code: str
    ts: float
    severity: str
    cleared: bool = False
    message: str = ""
    scope: str = ""
    group_id: str = ""
    synthetic: bool = False

    def key(self) -> tuple[str, str]:
        """通道与作用域组成的去重键（同站同类抖动算一路）。"""

        return (self.channel, self.scope)

    def as_dict(self) -> dict:
        return {
            "seq": self.seq,
            "channel": self.channel,
            "code": self.code,
            "ts": round(self.ts, 3),
            "severity": self.severity,
            "cleared": self.cleared,
            "message": self.message,
            "scope": self.scope,
            "group_id": self.group_id,
            "synthetic": self.synthetic,
        }


@dataclass
class AlarmGroup:
    """一条因果链上的一次扰动及其全部关联告警。"""

    group_id: str
    opened_ts: float
    last_ts: float
    state: str = GROUP_OPEN
    root_status: str = ROOT_PENDING
    root_code: str = ""
    root_seq: int = 0
    root_reason: str = ""
    observations: list[AlarmObservation] = field(default_factory=list)
    active_keys: dict[tuple[str, str], int] = field(default_factory=dict)
    expected: dict[str, float] = field(default_factory=dict)
    missing: list[str] = field(default_factory=list)
    closed_ts: float = 0.0
    root_decided_ts: float = 0.0
    recovered_ts: float = 0.0
    trip_qualified: bool = False
    tripped: bool = False
    acknowledged: bool = False
    ack_operator: str = ""
    ack_ts: float = 0.0

    def alarm_observations(self) -> list[AlarmObservation]:
        """返回组内告警发生帧（不含恢复帧与合成缺采帧）。"""

        return [
            item
            for item in self.observations
            if not item.cleared and not item.synthetic
        ]

    def add(self, observation: AlarmObservation) -> None:
        """把一条观测收进组，并维护当前仍在告警的通道集合。"""

        observation.group_id = self.group_id
        self.observations.append(observation)
        self.last_ts = max(self.last_ts, observation.ts)
        if observation.cleared:
            self.active_keys.pop(observation.key(), None)
        else:
            self.active_keys[observation.key()] = observation.seq

    def is_active(self) -> bool:
        """组是否尚未整组解除。"""

        return self.state != GROUP_RECOVERED

    def can_trip(self) -> bool:
        """是否具备停机资格：首因已标注（确认或推定）且首因码是停机码。"""

        return (
            self.state != GROUP_RECOVERED
            and self.root_status in (ROOT_CONFIRMED, ROOT_INFERRED)
            and self.trip_qualified
        )

    def as_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "policy": GROUPING_POLICY,
            "state": self.state,
            "opened_ts": round(self.opened_ts, 3),
            "last_ts": round(self.last_ts, 3),
            "closed_ts": round(self.closed_ts, 3) if self.closed_ts else None,
            "recovered_ts": round(self.recovered_ts, 3) if self.recovered_ts else None,
            "root_status": self.root_status,
            "root_code": self.root_code,
            "root_seq": self.root_seq,
            "root_reason": self.root_reason,
            "root_decided_ts": round(self.root_decided_ts, 3) if self.root_decided_ts else None,
            "trip_qualified": self.trip_qualified,
            "tripped": self.tripped,
            "acknowledged": self.acknowledged,
            "ack_operator": self.ack_operator,
            "observations": [item.as_dict() for item in self.observations],
            "active_channels": [
                {"channel": key[0], "scope": key[1], "seq": seq}
                for key, seq in sorted(self.active_keys.items(), key=lambda kv: kv[1])
            ],
            "expected_roots": sorted(self.expected),
            "missing_channels": list(self.missing),
            "alarm_count": len(self.alarm_observations()),
        }
