"""告警并组器：因果链并组、首因标注、缺采判定与整组解除。

并组口径与 ``catalog`` 钉死的一致——**因果链并组，时间窗只做闭窗与缺采判定**。
并组器订阅不到任何组件，它只接收平台归一化后的 :class:`AlarmObservation`，再把
并组生命周期事件发回事件总线。审计是总线的全量订阅者，因此"哪条并进哪组、首因
标给谁、缺了哪一路、哪组解除"与控制台读到的是同一份事实。

时序保证（全部在同一把可重入锁里顺序推进，序号单调不复用）：

1. 上报先按发生时间进缓冲，落后 ``order_lag_s`` 才放行，迟到帧不会插到旧标注前面；
2. 组状态单向 ``open → closed → recovered``，首因标注 ``pending → confirmed``，
   或 ``pending → inferred → confirmed``（迟到真首因可以升级，绝不降级）；
3. 停机资格只在首因标注完成后给出，且首因码必须是停机码，挂起中的组不停机；
4. 某路首因告警在缺采门限内没到，合成一条 ``channel.missing`` 告警——这是部分
   失败，阻塞整组自动解除，必须等补到或值班员确认，绝不能当成"没有告警"。
"""

from __future__ import annotations

import threading
from dataclasses import asdict

from ..clock import Clock, SystemClock
from ..config import AlarmLimits
from ..eventbus import Event, EventBus
from . import catalog
from .catalog import CHANNEL_MISSING, AlarmCode
from .group import (
    GROUP_CLOSED,
    GROUP_OPEN,
    GROUP_RECOVERED,
    ROOT_CONFIRMED,
    ROOT_INFERRED,
    ROOT_PENDING,
    AlarmGroup,
    AlarmObservation,
)

ALARM_TOPIC = "alarm"
ALARM_COMPONENT = "alarm"


class AlarmCorrelator:
    """把单路上报并成因果链告警组并维护整组生命周期。"""

    def __init__(self, limits: AlarmLimits, clock: Clock | None = None, bus: EventBus | None = None) -> None:
        self._limits = limits
        self._clock = clock or SystemClock()
        self._bus = bus
        self._lock = threading.RLock()
        self._groups: dict[str, AlarmGroup] = {}
        self._order: list[str] = []
        self._buffer: list[AlarmObservation] = []
        self._seq = 0
        self._serial = 0
        self._save_hook = None

    # ------------------------------------------------------------------

    def _now(self) -> float:
        return self._clock.now()

    def _publish(self, action: str, severity: str, message: str, **payload: object) -> None:
        if self._bus is None:
            return
        self._bus.publish(
            Event.of(
                ALARM_TOPIC,
                ALARM_COMPONENT,
                action,
                severity=severity,
                message=message,
                **payload,
            )
        )

    def _new_group_id(self, ts: float) -> str:
        self._serial += 1
        return f"alm-{int(ts)}-{self._serial:04d}"

    # ---- 上报入口 -----------------------------------------------------

    def report(
        self,
        channel: str,
        code: str,
        *,
        ts: float | None = None,
        severity: str | None = None,
        cleared: bool = False,
        message: str = "",
        scope: str = "",
    ) -> dict:
        """接收一路告警发生或恢复，返回本次处理结果描述。"""

        if not channel:
            raise ValueError("告警通道不能为空")
        if not code:
            raise ValueError("告警编码不能为空")
        moment = self._now() if ts is None else float(ts)
        grade = self._resolve_severity(code, severity, cleared)
        with self._lock:
            self._seq += 1
            observation = AlarmObservation(
                seq=self._seq,
                channel=str(channel),
                code=str(code),
                ts=moment,
                severity=grade,
                cleared=bool(cleared),
                message=str(message),
                scope=str(scope),
            )
            self._buffer.append(observation)
            self._buffer.sort(key=lambda item: item.ts)
            released = self._flush_buffer()
            return {
                "accepted_seq": observation.seq,
                "released": [item.as_dict() for item in released],
                "buffered": len(self._buffer),
            }

    @staticmethod
    def _resolve_severity(code: str, severity: str | None, cleared: bool) -> str:
        if cleared:
            return "notice"
        if severity is not None:
            if severity not in catalog.SEVERITY_WEIGHT:
                raise ValueError("严重度必须是 notice/warning/alarm")
            return severity
        entry = catalog.CODES.get(code)
        return entry.severity if entry is not None else "warning"

    def _flush_buffer(self) -> list[AlarmObservation]:
        """放行已经度过排序宽限的上报，按发生时间顺序处理。"""

        horizon = self._now() - self._limits.order_lag_s
        ready = [item for item in self._buffer if item.ts <= horizon]
        self._buffer = [item for item in self._buffer if item.ts > horizon]
        for observation in ready:
            self._process(observation)
        return ready

    # ---- 单条处理 -----------------------------------------------------

    def _process(self, obs: AlarmObservation) -> None:
        if obs.cleared:
            self._process_clear(obs)
        else:
            self._process_raised(obs)

    def _process_clear(self, obs: AlarmObservation) -> None:
        target = self._group_with_active(obs)
        if target is None:
            self._publish(
                "alarm_clear_orphan",
                "notice",
                f"恢复帧没有匹配到在役告警：{obs.channel}",
                channel=obs.channel,
                code=obs.code,
                scope=obs.scope,
                ts=round(obs.ts, 3),
            )
            return
        target.add(obs)
        self._publish_observed(obs)
        self._try_recover(target)
        self._persist_later()

    def _group_with_active(self, obs: AlarmObservation) -> AlarmGroup | None:
        for group_id in reversed(self._order):
            group = self._groups[group_id]
            if not group.is_active():
                continue
            active = group.active_keys.get((obs.channel, obs.scope))
            if active is not None:
                return group
            # 恢复帧可能只按编码 + 作用域上报（通道名归一化差异）。
            for key in group.active_keys:
                if key[1] == obs.scope and self._active_code(group, key) == obs.code:
                    return group
        return None

    @staticmethod
    def _active_code(group: AlarmGroup, key: tuple[str, str]) -> str:
        seq = group.active_keys[key]
        for item in group.observations:
            if item.seq == seq:
                return item.code
        return ""

    def _process_raised(self, obs: AlarmObservation) -> None:
        # 1) 同一路抖动：该通道在某个在役组里仍处于告警态，续记到原组，不另开组。
        chatter = self._active_group_by_key(obs.channel, obs.scope)
        if chatter is not None:
            chatter.add(obs)
            self._after_raised(chatter, obs, action="chatter")
            return

        # 2) 因果链并组，只走因果表里的直接边；时间窗只判断是否同一次扰动，
        #    时间再近、因果图里不相连的告警也绝不并组。
        direct = self._direct_matches(obs)
        if direct:
            target = self._merge_if_needed(direct)
            target.add(obs)
            self._after_raised(target, obs, action="attached")
            return

        # 3) 先开一个首因挂起的新组；如果它的可能首因已经在别的挂起组里等着，
        #    把两个挂起组并起来（传递关系只能在首因这一层汇合，不能越级直挂）。
        group = AlarmGroup(group_id=self._new_group_id(obs.ts), opened_ts=obs.ts, last_ts=obs.ts)
        self._groups[group.group_id] = group
        self._order.append(group.group_id)
        group.add(obs)
        self._refresh_expected(group, obs)
        pending = self._pending_root_matches(group, obs)
        if pending:
            target = self._merge_if_needed([group, *pending])
            self._after_raised(target, obs, action="attached")
            return
        # 迟到的真首因：别的组已经挂起到点推定了，仍按因果链接回并升级首因标注。
        late = self._late_root_matches(group, obs)
        if late:
            target = self._merge_if_needed([group, *late])
            self._after_raised(target, obs, action="attached")
            return
        self._after_raised(group, obs, action="created")

    def _direct_matches(self, obs: AlarmObservation) -> list[AlarmGroup]:
        """只按因果表声明的直接原因码在因果窗内找组。"""

        causes = catalog.direct_causes(obs.code)
        if not causes:
            return []
        groups: list[AlarmGroup] = []
        for group in self._receivable_groups():
            for earlier in group.observations:
                if earlier.cleared or earlier.synthetic:
                    continue
                if earlier.code not in causes:
                    continue
                delta = obs.ts - earlier.ts
                if delta < 0.0 or delta > self._limits.causal_window_s:
                    continue
                if not self._scope_compatible(earlier, obs):
                    continue
                groups.append(group)
                break
        return groups

    def _pending_root_matches(self, group: AlarmGroup, obs: AlarmObservation) -> list[AlarmGroup]:
        """新挂起组的可能首因，是否已在别的在役组的"等首因"名单里且仍在窗内。"""

        roots = catalog.root_ancestors(obs.code)
        if not roots:
            return []
        return self._groups_awaiting_roots(roots, obs, group, ROOT_PENDING)

    def _late_root_matches(self, group: AlarmGroup, obs: AlarmObservation) -> list[AlarmGroup]:
        """真根因码迟到：并回已经推定首因、但一直在等该根因的组。"""

        if not catalog.is_root_code(obs.code):
            return []
        return self._groups_awaiting_roots((obs.code,), obs, group, ROOT_INFERRED)

    def _groups_awaiting_roots(
        self,
        roots: tuple[str, ...],
        obs: AlarmObservation,
        group: AlarmGroup,
        root_status: str,
    ) -> list[AlarmGroup]:
        keys = {
            f"{root}@{obs.scope}" if catalog.CODES[root].scoped and obs.scope else root
            for root in roots
        }
        matches: list[AlarmGroup] = []
        for other in self._receivable_groups():
            if other is group or other.root_status != root_status:
                continue
            if not keys.intersection(other.expected):
                continue
            if obs.ts - other.last_ts > self._limits.causal_window_s:
                continue
            matches.append(other)
        return matches

    def _active_group_by_key(self, channel: str, scope: str) -> AlarmGroup | None:
        for group_id in reversed(self._order):
            group = self._groups[group_id]
            if group.is_active() and (channel, scope) in group.active_keys:
                return group
        return None

    def _receivable_groups(self) -> list[AlarmGroup]:
        """可并入的组：所有未解除的组。

        闭窗后的组仍允许补录迟到的因果告警，是否真能并由
        :meth:`_causal_matches` 逐条核对"直接原因 + 因果窗 + 作用域"，恢复后的组
        一律不接收新告警。
        """

        return [self._groups[gid] for gid in self._order if self._groups[gid].state != GROUP_RECOVERED]

    @staticmethod
    def _scope_compatible(cause_obs: AlarmObservation, effect_obs: AlarmObservation) -> bool:
        cause_entry = catalog.CODES.get(cause_obs.code)
        effect_entry = catalog.CODES.get(effect_obs.code)
        cause_scoped = cause_entry.scoped if cause_entry else False
        # 全局（非按站）原因向任何站传播；两端都按站时必须同站，不能跨站串线。
        if not cause_scoped:
            return True
        return bool(effect_obs.scope) and cause_obs.scope == effect_obs.scope

    def _merge_if_needed(self, matched: list[AlarmGroup]) -> AlarmGroup:
        unique: list[AlarmGroup] = []
        for group in matched:
            if group not in unique:
                unique.append(group)
        if len(unique) == 1:
            return unique[0]
        target = min(unique, key=lambda group: (group.opened_ts, group.group_id))
        for group in unique:
            if group is target:
                continue
            self._merge_group(target, group)
        return target

    def _merge_group(self, target: AlarmGroup, other: AlarmGroup) -> None:
        moved = [item for item in other.observations]
        for item in moved:
            target.add(item)
        target.opened_ts = min(target.opened_ts, other.opened_ts)
        for code, deadline in other.expected.items():
            target.expected[code] = min(target.expected.get(code, deadline), deadline)
        for missing in other.missing:
            if missing not in target.missing:
                target.missing.append(missing)
        if other.acknowledged:
            target.acknowledged = True
            target.ack_operator = other.ack_operator
            target.ack_ts = other.ack_ts
        self._groups.pop(other.group_id, None)
        self._order.remove(other.group_id)
        self._publish(
            "alarm_group_merged",
            "notice",
            f"告警组 {other.group_id} 因果相连，并入 {target.group_id}",
            group_id=target.group_id,
            merged_from=other.group_id,
        )

    def _after_raised(self, group: AlarmGroup, obs: AlarmObservation, action: str) -> None:
        self._refresh_expected(group, obs)
        self._resolve_late_missing(group, obs)
        self._reevaluate_root(group)
        if action == "created":
            self._publish(
                "alarm_group_opened",
                "notice",
                f"新开告警组 {group.group_id}（首因挂起）",
                group_id=group.group_id,
                opened_ts=round(group.opened_ts, 3),
            )
        self._publish_observed(obs)
        if action in ("created", "attached"):
            self._publish(
                f"alarm_{action}",
                obs.severity,
                obs.message or f"{obs.code} 并入 {group.group_id}",
                group_id=group.group_id,
                channel=obs.channel,
                code=obs.code,
                scope=obs.scope,
                seq=obs.seq,
            )
        if self._limits.unknown_root_policy == "heaviest-immediate":
            self._infer_root_if_pending(group, obs.ts)
        self._try_recover(group)
        self._persist_later()

    # ---- 首因与缺采 ---------------------------------------------------

    def _refresh_expected(self, group: AlarmGroup, obs: AlarmObservation) -> None:
        for root in catalog.root_ancestors(obs.code):
            entry = catalog.CODES[root]
            if entry.scoped:
                scopes = {obs.scope} if obs.scope else {item.scope for item in group.observations if item.scope}
                for scope in scopes:
                    key = f"{root}@{scope}"
                    group.expected.setdefault(key, obs.ts + self._limits.missing_after_s)
            else:
                group.expected.setdefault(root, obs.ts + self._limits.missing_after_s)

    def _resolve_late_missing(self, group: AlarmGroup, obs: AlarmObservation) -> None:
        """缺采判定后真首因补到：解除该路缺采，但缺采事实留在组史里。"""

        if catalog.is_root_code(obs.code):
            key = f"{obs.code}@{obs.scope}" if obs.scope else obs.code
            if key in group.missing:
                group.missing.remove(key)
                group.expected.pop(key, None)
                clear = AlarmObservation(
                    seq=0,
                    channel=obs.code,
                    code=CHANNEL_MISSING.code,
                    ts=obs.ts,
                    severity="notice",
                    cleared=True,
                    message=f"缺采通道补到：{key}",
                    scope=obs.scope,
                    synthetic=True,
                )
                self._seq += 1
                clear.seq = self._seq
                group.add(clear)
                self._publish(
                    "alarm_channel_recovered",
                    "notice",
                    f"缺采通道补到，解除部分失败标记：{key}",
                    group_id=group.group_id,
                    expected=key,
                    seq=clear.seq,
                )

    def _reevaluate_root(self, group: AlarmGroup) -> None:
        roots = [
            item
            for item in group.alarm_observations()
            if catalog.is_root_code(item.code)
        ]
        if not roots:
            return
        chosen = min(roots, key=lambda item: item.ts)
        # 真根因一旦到达，就从挂起/推定升级为确认（真因优先于推定），且最早到达的
        # 真根因是最终首因；确认后的标注不再回退。
        if group.root_status != ROOT_CONFIRMED:
            self._mark_root(group, chosen, ROOT_CONFIRMED, "首因按因果链确认")

    def _infer_root_if_pending(self, group: AlarmGroup, ts: float) -> None:
        if group.root_status != ROOT_PENDING:
            return
        candidates = group.alarm_observations()
        if not candidates:
            return
        chosen = max(candidates, key=lambda item: (self._weight(item), -item.seq))
        self._mark_root(
            group,
            chosen,
            ROOT_INFERRED,
            "首因在挂起窗内未到达，按最重一条推定",
            decided_ts=ts,
        )

    def _weight(self, obs: AlarmObservation) -> int:
        entry = catalog.CODES.get(obs.code)
        if entry is not None:
            return entry.weight()
        return catalog.SEVERITY_WEIGHT.get(obs.severity, 1)

    def _mark_root(
        self,
        group: AlarmGroup,
        obs: AlarmObservation,
        status: str,
        reason: str,
        decided_ts: float | None = None,
    ) -> None:
        previous = group.root_status
        group.root_status = status
        group.root_code = obs.code
        group.root_seq = obs.seq
        group.root_reason = reason
        group.root_decided_ts = decided_ts if decided_ts is not None else obs.ts
        entry = catalog.CODES.get(obs.code)
        group.trip_qualified = bool(entry.trip) if entry is not None else False
        if status == ROOT_CONFIRMED:
            # 真首因已到：因果链上的其余"可能首因"全部排除，既不再等待也不判缺采；
            # 缺采只对"首因未明"时有意义，首因明确后不存在"该来没来的首因"。
            group.expected = {}
            for item in list(group.observations):
                if item.synthetic and item.code == CHANNEL_MISSING.code and not item.cleared:
                    item.cleared = True
                    group.active_keys.pop(item.key(), None)
            group.missing = []
        self._publish(
            "alarm_root_marked",
            "alarm",
            f"{group.group_id} 首因标注：{obs.code}（{status}）",
            group_id=group.group_id,
            root_code=obs.code,
            root_seq=obs.seq,
            root_status=status,
            previous_status=previous,
            trip_qualified=group.trip_qualified,
            reason=reason,
            channel=obs.channel,
            scope=obs.scope,
        )

    # ---- 周期推进 -----------------------------------------------------

    def tick(self, now: float | None = None) -> dict:
        """推进时序：放行迟到帧、闭窗、缺采判定、挂起到点推定、整组解除。"""

        with self._lock:
            moment = self._now() if now is None else float(now)
            self._flush_buffer()
            closed: list[str] = []
            missing_marked: list[str] = []
            inferred: list[str] = []
            recovered: list[str] = []
            for group_id in list(self._order):
                group = self._groups[group_id]
                if not group.is_active():
                    continue
                if group.state == GROUP_OPEN and group.opened_ts + self._limits.group_close_s <= moment:
                    group.state = GROUP_CLOSED
                    group.closed_ts = moment
                    closed.append(group_id)
                    self._publish(
                        "alarm_group_closed",
                        "notice",
                        f"告警组 {group_id} 闭窗封组",
                        group_id=group_id,
                        closed_ts=round(moment, 3),
                    )
                newly_missing = self._check_missing(group, moment)
                if newly_missing:
                    missing_marked.extend(newly_missing)
                if (
                    group.root_status == ROOT_PENDING
                    and self._limits.unknown_root_policy == "hold-then-heaviest"
                    and group.opened_ts + self._limits.root_hold_s <= moment
                ):
                    self._infer_root_if_pending(group, moment)
                    if group.root_status == ROOT_INFERRED:
                        inferred.append(group_id)
                if self._try_recover(group, moment):
                    recovered.append(group_id)
            self._persist_later()
            return {
                "closed": closed,
                "missing": missing_marked,
                "root_inferred": inferred,
                "recovered": recovered,
            }

    def _check_missing(self, group: AlarmGroup, now: float) -> list[str]:
        declared: list[str] = []
        # 首因一旦明确，其它候选首因就不是"该来没来"，直接从待观察名单剔除，
        # 绝不允许在首因已确认后再合成缺采告警。
        if group.root_status == ROOT_CONFIRMED:
            group.expected = {}
            group.missing = []
            return declared
        for key, deadline in list(group.expected.items()):
            if key in group.missing:
                continue
            if self._expected_seen(group, key):
                group.expected.pop(key, None)
                continue
            if deadline <= now:
                group.missing.append(key)
                code, _, scope = key.partition("@")
                self._seq += 1
                synthetic = AlarmObservation(
                    seq=self._seq,
                    channel=code,
                    code=CHANNEL_MISSING.code,
                    ts=now,
                    severity="alarm",
                    message=f"上游首因告警通道缺采（部分失败）：{key}",
                    scope=scope,
                    synthetic=True,
                )
                group.add(synthetic)
                declared.append(key)
                self._publish(
                    "alarm_channel_missing",
                    "alarm",
                    f"告警组 {group.group_id} 上游通道缺采：{key}，按部分失败处理",
                    group_id=group.group_id,
                    expected=key,
                    root_code=code,
                    scope=scope,
                    seq=synthetic.seq,
                )
        return declared

    @staticmethod
    def _expected_seen(group: AlarmGroup, key: str) -> bool:
        code, _, scope = key.partition("@")
        for item in group.alarm_observations():
            if item.code == code and item.scope == scope:
                return True
        return False

    def _try_recover(self, group: AlarmGroup, now: float | None = None) -> bool:
        if not group.is_active():
            return False
        if group.active_keys:
            return False
        # 同一路重复抖动至少有一帧发生、全部恢复才算现场平复；空组不解除。
        if not group.alarm_observations():
            return False
        if group.missing:
            return False
        moment = self._now() if now is None else now
        group.state = GROUP_RECOVERED
        group.recovered_ts = moment
        self._publish(
            "alarm_group_recovered",
            "notice",
            f"告警组 {group.group_id} 全部告警解除，整组恢复",
            group_id=group.group_id,
            recovered_ts=round(moment, 3),
            root_code=group.root_code,
            root_status=group.root_status,
            had_trip=group.tripped,
        )
        return True

    # ---- 停机依据 -----------------------------------------------------

    def due_trips(self, now: float | None = None) -> list[dict]:
        """返回首因已标注、首因码具备停机资格且尚未停机的组（取一次即标记）。"""

        with self._lock:
            self.tick(now)
            due: list[dict] = []
            for group_id in self._order:
                group = self._groups[group_id]
                if group.can_trip() and not group.tripped:
                    group.tripped = True
                    due.append(
                        {
                            "group_id": group_id,
                            "root_code": group.root_code,
                            "root_seq": group.root_seq,
                            "root_status": group.root_status,
                            "opened_ts": round(group.opened_ts, 3),
                        }
                    )
            self._persist_later()
            return due

    def mark_trip_skipped(self, group_id: str) -> None:
        """外部已手动停机时标记该组，避免自动停机重复触发。"""

        with self._lock:
            group = self._groups[group_id]
            group.tripped = True

    def tripped_group(self) -> dict | None:
        """返回导致当前停机的首因组（没有则返回 None）。"""

        for group_id in reversed(self._order):
            group = self._groups[group_id]
            if group.tripped and group.is_active():
                return group.as_dict()
        return None

    # ---- 值班确认 -----------------------------------------------------

    def acknowledge(self, group_id: str, operator: str, now: float | None = None) -> dict:
        """值班员确认一个被缺采阻塞的在役组（缺采通道无法自动判定恢复时使用）。"""

        with self._lock:
            group = self._groups[group_id]
            moment = self._now() if now is None else float(now)
            group.acknowledged = True
            group.ack_operator = str(operator)
            group.ack_ts = moment
            self._publish(
                "alarm_group_acknowledged",
                "warning",
                f"{operator} 确认告警组 {group_id}（缺采 {len(group.missing)} 路）",
                group_id=group_id,
                operator=operator,
                missing=list(group.missing),
            )
            self._persist_later()
            return group.as_dict()

    # ---- 查询 ---------------------------------------------------------

    def groups(self, active_only: bool = False) -> list[dict]:
        with self._lock:
            self._flush_buffer()
            selected = [self._groups[gid] for gid in self._order]
            if active_only:
                selected = [group for group in selected if group.is_active()]
            return [group.as_dict() for group in selected]

    def active_groups(self) -> list[dict]:
        return self.groups(active_only=True)

    def get(self, group_id: str) -> dict:
        with self._lock:
            return self._groups[group_id].as_dict()

    def summary(self) -> dict:
        with self._lock:
            groups = [self._groups[gid] for gid in self._order]
            active = [group for group in groups if group.is_active()]
            pending = [group for group in active if group.root_status == ROOT_PENDING]
            tripped = [group for group in active if group.tripped]
            return {
                "policy": "causal-chain",
                "total": len(groups),
                "active": len(active),
                "open": sum(1 for group in active if group.state == GROUP_OPEN),
                "closed": sum(1 for group in active if group.state == GROUP_CLOSED),
                "recovered": sum(1 for group in groups if group.state == GROUP_RECOVERED),
                "root_pending": len(pending),
                "tripped": len(tripped),
                "partial_failures": sum(len(group.missing) for group in active),
                "buffered": len(self._buffer),
                "seq": self._seq,
            }

    def _publish_observed(self, obs: AlarmObservation) -> None:
        self._publish(
            "alarm_observed",
            obs.severity,
            obs.message or f"{obs.code} 上报",
            group_id=obs.group_id,
            channel=obs.channel,
            code=obs.code,
            scope=obs.scope,
            seq=obs.seq,
            ts=round(obs.ts, 3),
            cleared=obs.cleared,
            synthetic=obs.synthetic,
        )

    # ---- 持久化 -------------------------------------------------------

    def _persist_later(self) -> None:
        # 持久化由平台在锁外统一落盘，避免并组器反向依赖存储层。
        if self._save_hook is not None:
            self._save_hook(self.snapshot())

    def set_save_hook(self, hook) -> None:
        """注册快照回调（平台用它把并组当前态写进 DurableState）。"""

        self._save_hook = hook

    def snapshot(self) -> dict:
        """导出可落盘的并组当前态。"""

        with self._lock:
            return {
                "seq": self._seq,
                "serial": self._serial,
                "order": list(self._order),
                "buffer": [asdict(item) for item in self._buffer],
                "groups": [
                    {
                        "group_id": group.group_id,
                        "opened_ts": group.opened_ts,
                        "last_ts": group.last_ts,
                        "state": group.state,
                        "root_status": group.root_status,
                        "root_code": group.root_code,
                        "root_seq": group.root_seq,
                        "root_reason": group.root_reason,
                        "observations": [asdict(item) for item in group.observations],
                        "active_keys": [[key[0], key[1], seq] for key, seq in group.active_keys.items()],
                        "expected": dict(group.expected),
                        "missing": list(group.missing),
                        "closed_ts": group.closed_ts,
                        "root_decided_ts": group.root_decided_ts,
                        "recovered_ts": group.recovered_ts,
                        "trip_qualified": group.trip_qualified,
                        "tripped": group.tripped,
                        "acknowledged": group.acknowledged,
                        "ack_operator": group.ack_operator,
                        "ack_ts": group.ack_ts,
                    }
                    for group in (self._groups[gid] for gid in self._order)
                ],
            }

    def restore(self, raw: dict) -> None:
        """从落盘当前态恢复并组器（重启后沿用原组号与首因标注）。"""

        if not isinstance(raw, dict):
            return
        with self._lock:
            self._seq = int(raw.get("seq", 0))
            self._serial = int(raw.get("serial", 0))
            self._buffer = [AlarmObservation(**item) for item in raw.get("buffer", [])]
            self._order = []
            self._groups = {}
            for item in raw.get("groups", []):
                observations = [AlarmObservation(**obs) for obs in item.get("observations", [])]
                group = AlarmGroup(
                    group_id=item["group_id"],
                    opened_ts=float(item["opened_ts"]),
                    last_ts=float(item["last_ts"]),
                    state=item.get("state", GROUP_OPEN),
                    root_status=item.get("root_status", ROOT_PENDING),
                    root_code=item.get("root_code", ""),
                    root_seq=int(item.get("root_seq", 0)),
                    root_reason=item.get("root_reason", ""),
                    observations=observations,
                    active_keys={
                        (entry[0], entry[1]): int(entry[2])
                        for entry in item.get("active_keys", [])
                    },
                    expected=dict(item.get("expected", {})),
                    missing=list(item.get("missing", [])),
                    closed_ts=float(item.get("closed_ts", 0.0)),
                    root_decided_ts=float(item.get("root_decided_ts", 0.0)),
                    recovered_ts=float(item.get("recovered_ts", 0.0)),
                    trip_qualified=bool(item.get("trip_qualified", False)),
                    tripped=bool(item.get("tripped", False)),
                    acknowledged=bool(item.get("acknowledged", False)),
                    ack_operator=item.get("ack_operator", ""),
                    ack_ts=float(item.get("ack_ts", 0.0)),
                )
                self._groups[group.group_id] = group
                self._order.append(group.group_id)


def code_label(code: str) -> str:
    """供审计正文使用的告警码中文标签。"""

    entry: AlarmCode | None = catalog.CODES.get(code)
    return entry.label if entry is not None else code
