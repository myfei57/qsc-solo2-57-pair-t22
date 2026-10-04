"""告警并组器。

订阅事件总线上的 ``alarm`` 与 ``heartbeat`` 两类事件，把时间上挨着的告警并成组、
标出首因、给出停堆依据，并把每一步派生结论发回总线。审计日志是总线的全量订阅者，
因此控制台展示、停堆依据、审计流水天然落在同一条事件链上，三者不可能各说各话。

写死的设计（改动等于改安全案）：

1. **并组按时间窗，不按因果链**。组开着时，新告警与组内最近一次告警相距不超过
   ``window_s`` 就并入，否则另开新组；因果规则只用于组内首因排序。
2. **首因标注是确定性全序**：先剔除被组内成员因果导致的告警，剩下的候选里唯一者
   即首因；多候选取最先报出的（与次先者相差超过 ``simultaneity_epsilon_s`` 才算
   分得清先后）；分不清先后即"首因不明"，按系统配置 ``unclear_policy`` 处置——
   ``report_worst`` 报最重的一条并标"暂定"（默认，停堆不等人），``hold`` 挂起
   （停堆依据不可执行，等成员变化后重算）。最终平局按
   （严重度， 时间， 序号， 告警名） 全序裁决，同样的输入永远得到同一个首因。
3. **处理流水线顺序固定**：收口过期组 → 入组 → 标注 → 停堆依据 → 解除评估 →
   发事件。摄入时间必须单调，回退直接拒绝，不允许带病重排。
4. **通道缺失是部分失败，不是无告警**。通道心跳超时即降级，系统健康转入
   ``partial``；证据不全时组不得判解除，审计留下 ``clear_blocked``。
"""

from __future__ import annotations

import hashlib
import threading

from ..clock import Clock
from ..config import AlarmLimits
from ..errors import AlarmError
from ..eventbus import Event, EventBus
from .rules import RuleSet
from .state import (
    CERTAIN,
    CLEARED,
    CLOSED,
    HOLD,
    OPEN,
    PENDING,
    PROVISIONAL,
    SEVERITY_WEIGHT,
    Group,
    Member,
    RootCauseLabel,
    ShutdownBasis,
)

TOPIC_ALARM = "alarm"
TOPIC_HEARTBEAT = "heartbeat"
TOPIC_GROUP = "alarm.group"


def compute_label(group: Group, rules: RuleSet, limits: AlarmLimits) -> RootCauseLabel | None:
    """纯函数：同一成员集 + 同一规则版本 + 同一配置 => 同一张首因标注。

    控制台、停堆依据与审计都从这一个函数取结果，这是三方一致性的根。
    """

    if not group.members:
        return None
    present = set(group.members)
    candidates = [m for m in group.members.values() if not rules.caused_by_member(m.alarm_id, present)]
    prefix = ""
    if not candidates:
        # 因果成环（规则把组内成员互相指认）：确定性回退到全体成员，按同一全序裁决。
        candidates = list(group.members.values())
        prefix = "cycle-fallback:"
    order = sorted(candidates, key=lambda m: (m.first_ts, m.first_seq, m.alarm_id))
    version = (group.member_seq, rules.version)
    if len(order) == 1:
        return RootCauseLabel(order[0].alarm_id, CERTAIN, prefix + "sole-candidate", *version)
    first, second = order[0], order[1]
    if second.first_ts - first.first_ts > limits.simultaneity_epsilon_s:
        return RootCauseLabel(first.alarm_id, CERTAIN, prefix + "first-out", *version)
    if limits.unclear_policy == HOLD:
        return RootCauseLabel(None, PENDING, prefix + "root-ambiguous:hold", *version)
    tied = [m for m in order if m.first_ts - first.first_ts <= limits.simultaneity_epsilon_s]
    worst = min(tied, key=lambda m: (-SEVERITY_WEIGHT[m.severity], m.first_ts, m.first_seq, m.alarm_id))
    return RootCauseLabel(worst.alarm_id, PROVISIONAL, prefix + "root-ambiguous:worst-of-tie", *version)


def members_digest(group: Group) -> str:
    """成员集摘要：首因标注与停堆依据都带它，审计可按摘要对账。"""

    ordered = sorted(group.members.values(), key=lambda m: (m.first_ts, m.first_seq))
    body = ";".join(f"{m.alarm_id}:{m.first_ts}:{m.first_seq}" for m in ordered)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]


class AlarmCorrelator:
    """告警并组器：订阅总线、维护组状态机、给出停堆依据。"""

    def __init__(self, bus: EventBus, clock: Clock, limits: AlarmLimits, rules: RuleSet) -> None:
        self._bus = bus
        self._clock = clock
        self._limits = limits
        self._rules = rules
        self._groups: list[Group] = []
        self._channels: dict[str, float] = {}
        self._degraded: set[str] = set()
        self._counter = 0
        self._seq = 0
        self._last_ts: float | None = None
        self._last_basis: object = object()  # 哨兵：第一次评估必发一条停堆依据
        self._lock = threading.RLock()
        bus.subscribe(TOPIC_ALARM, self._on_alarm)
        bus.subscribe(TOPIC_HEARTBEAT, self._on_heartbeat)

    @property
    def rules(self) -> RuleSet:
        return self._rules

    @property
    def limits(self) -> AlarmLimits:
        return self._limits

    # ---- 总线入口 -----------------------------------------------------

    def _on_alarm(self, event: Event) -> None:
        payload = event.payload
        alarm_id = str(payload.get("alarm_id", ""))
        state = str(payload.get("state", ""))
        channel = str(payload.get("channel", "")) or event.component
        if not alarm_id or state not in ("active", "cleared"):
            self._emit(
                "alarm_rejected",
                severity="warning",
                message=f"告警事件缺少 alarm_id 或 state 非法，已拒收: {payload}",
            )
            return
        severity = str(payload.get("severity", "warning"))
        if severity not in SEVERITY_WEIGHT:
            severity = "warning"
        with self._lock:
            now = self._advance()
            if state == "active":
                self._activate(now, alarm_id, severity, channel, bool(payload.get("momentary", False)))
            else:
                self._clear(now, alarm_id, channel)

    def _on_heartbeat(self, event: Event) -> None:
        channel = str(event.payload.get("channel", "")) or event.component
        with self._lock:
            now = self._advance()
            self._touch_channel(channel, now)
            self._reevaluate(now)

    # ---- 外部驱动 -----------------------------------------------------

    def tick(self) -> None:
        """推进并组状态机：收口过期组、刷新通道健康、重估解除。控制周期末尾调用。"""

        with self._lock:
            now = self._advance()
            self._expire(now)
            self._reevaluate(now)

    # ---- 查询 ---------------------------------------------------------

    def groups(self) -> list[Group]:
        """返回全部告警组（含已解除），按开窗先后排列。"""

        with self._lock:
            return list(self._groups)

    def health(self) -> dict:
        """系统健康：任一通道降级即 ``partial``——缺一路是部分失败，不是无告警。"""

        with self._lock:
            now = self._clock.now()
            degraded = sorted(c for c in self._channels if self._is_degraded(c, now))
        return {"status": "partial" if degraded else "ok", "degraded_channels": degraded}

    def shutdown_basis(self) -> ShutdownBasis | None:
        """停堆依据：只认首因那一组（最近一个未解除的组），没有则返回 ``None``。"""

        with self._lock:
            active = [g for g in self._groups if g.status != CLEARED]
            if not active:
                return None
            group = active[-1]
            label = group.label or compute_label(group, self._rules, self._limits)
            if label is None:
                return None
            root_severity = (
                group.members[label.root].severity if label.root in group.members else "notice"
            )
            return ShutdownBasis(
                group_id=group.group_id,
                root=label.root,
                root_severity=root_severity,
                confidence=label.confidence,
                actionable=label.confidence != PENDING,
                reason=label.reason,
                member_seq=group.member_seq,
                rule_version=self._rules.version,
                members_digest=members_digest(group),
            )

    # ---- 状态机 -------------------------------------------------------

    def _advance(self) -> float:
        """取当前时间并强制单调：回退即拒绝，时序不许乱。"""

        now = self._clock.now()
        if self._last_ts is not None and now < self._last_ts:
            raise AlarmError(
                f"告警时序回退: {now:.3f} < {self._last_ts:.3f}", component="alarm"
            )
        self._last_ts = now
        return now

    def _touch_channel(self, channel: str, now: float) -> None:
        """通道任何一次发声（告警、清除、心跳）都算活着。"""

        self._channels[channel] = max(now, self._channels.get(channel, now))

    def _is_degraded(self, channel: str, now: float) -> bool:
        last = self._channels.get(channel)
        return last is None or (now - last) > self._limits.heartbeat_timeout_s

    def _open_group(self) -> Group | None:
        return self._groups[-1] if self._groups and self._groups[-1].status == OPEN else None

    def _expire(self, now: float) -> None:
        for group in self._groups:
            if group.status == OPEN and now - group.last_active_ts > self._limits.window_s:
                group.status = CLOSED
                self._emit(
                    "group_closed",
                    group=group.group_id,
                    members=len(group.members),
                    members_digest=members_digest(group),
                    message=f"告警组 {group.group_id} 时间窗收口，成员冻结",
                )

    def _activate(self, now: float, alarm_id: str, severity: str, channel: str, momentary: bool) -> None:
        self._seq += 1
        self._touch_channel(channel, now)
        self._expire(now)
        group = self._open_group()
        if group is None:
            self._counter += 1
            group = Group(group_id=f"AG{self._counter:04d}", opened_ts=now, last_active_ts=now)
            self._groups.append(group)
            self._emit(
                "group_opened",
                group=group.group_id,
                alarm=alarm_id,
                message=f"告警组 {group.group_id} 开窗",
            )
        member = group.members.get(alarm_id)
        if member is None:
            member = Member(alarm_id, channel, severity, now, self._seq, cleared=momentary, momentary=momentary)
            group.members[alarm_id] = member
            group.member_seq += 1
            self._emit(
                "member_added",
                severity=severity,
                group=group.group_id,
                alarm=alarm_id,
                channel=channel,
                member_seq=group.member_seq,
                message=f"告警 {alarm_id} 并入 {group.group_id}",
            )
        else:
            member.occurrences += 1
            member.cleared = member.momentary
            if member.severity != severity:
                member.severity = severity
                group.member_seq += 1
            self._emit(
                "member_refired",
                severity=severity,
                group=group.group_id,
                alarm=alarm_id,
                occurrences=member.occurrences,
                message=f"告警 {alarm_id} 重复报出（第 {member.occurrences} 次），仍属 {group.group_id}",
            )
        group.last_active_ts = now
        self._reevaluate(now)

    def _clear(self, now: float, alarm_id: str, channel: str) -> None:
        self._touch_channel(channel, now)
        self._expire(now)
        found = False
        for group in self._groups:
            if group.status == CLEARED:
                continue
            member = group.members.get(alarm_id)
            if member is not None and not member.cleared:
                member.cleared = True
                found = True
                self._emit(
                    "member_cleared",
                    group=group.group_id,
                    alarm=alarm_id,
                    message=f"告警 {alarm_id} 在 {group.group_id} 内解除",
                )
        if not found:
            self._emit(
                "orphan_clear",
                severity="warning",
                alarm=alarm_id,
                channel=channel,
                message=f"收到 {alarm_id} 的解除，但没有对应在报告警，按孤儿解除落审计",
            )
        self._reevaluate(now)

    def _reevaluate(self, now: float) -> None:
        """固定顺序：通道健康 → 首因标注 → 解除评估 → 停堆依据。"""

        self._update_channels(now)
        for group in self._groups:
            if group.status == CLEARED:
                continue
            label = compute_label(group, self._rules, self._limits)
            if label != group.label:
                group.label = label
                if label is not None:
                    self._emit(
                        "root_labeled",
                        severity="warning",
                        group=group.group_id,
                        root=label.root or "",
                        confidence=label.confidence,
                        reason=label.reason,
                        member_seq=group.member_seq,
                        rule_version=self._rules.version,
                        members_digest=members_digest(group),
                        message=f"告警组 {group.group_id} 首因标注: {label.root or '待定'}（{label.confidence}）",
                    )
            if group.status == CLOSED:
                self._eval_clearance(group, now)
        basis = self.shutdown_basis()
        if basis != self._last_basis:
            self._last_basis = basis
            if basis is None:
                self._emit("shutdown_basis", group="", actionable=False, reason="no-active-group",
                           message="当前没有未解除的告警组，无停堆依据")
            else:
                self._emit(
                    "shutdown_basis",
                    severity="alarm" if basis.actionable and basis.root_severity == "alarm" else "warning",
                    **basis.as_dict(),
                    message=(
                        f"停堆依据: 组 {basis.group_id} 首因 {basis.root or '待定'}"
                        f"（{'可执行' if basis.actionable else '挂起'}）"
                    ),
                )

    def _eval_clearance(self, group: Group, now: float) -> None:
        if not all(member.cleared for member in group.members.values()):
            return
        complete, degraded = self._evidence_complete(group, now)
        if not complete:
            if not group.clear_blocked:
                group.clear_blocked = True
                self._emit(
                    "clear_blocked",
                    severity="warning",
                    group=group.group_id,
                    degraded_channels=degraded,
                    message=f"告警组 {group.group_id} 成员已解除但证据不全（通道 {degraded} 缺失），暂不判解除",
                )
            return
        group.status = CLEARED
        group.cleared_ts = now
        self._emit(
            "group_cleared",
            group=group.group_id,
            message=f"告警组 {group.group_id} 全部成员解除且证据完整，确认解除",
        )

    def _evidence_complete(self, group: Group, now: float) -> tuple[bool, list[str]]:
        channels = {member.channel for member in group.members.values()}
        if self._limits.clearance_requires_full_health:
            channels |= set(self._channels)
        degraded = sorted(c for c in channels if self._is_degraded(c, now))
        return (not degraded, degraded)

    def _update_channels(self, now: float) -> None:
        for channel in self._channels:
            degraded = self._is_degraded(channel, now)
            if degraded and channel not in self._degraded:
                self._degraded.add(channel)
                self._emit(
                    "channel_degraded",
                    severity="warning",
                    channel=channel,
                    message=f"通道 {channel} 心跳缺失，按部分失败处理（不是无告警）",
                )
            elif not degraded and channel in self._degraded:
                self._degraded.discard(channel)
                self._emit(
                    "channel_recovered",
                    channel=channel,
                    message=f"通道 {channel} 心跳恢复",
                )

    def _emit(self, action: str, severity: str = "notice", **payload) -> None:
        self._bus.publish(
            Event.of(TOPIC_GROUP, "alarm", action, severity=severity, **payload)
        )
