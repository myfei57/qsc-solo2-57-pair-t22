"""并组结果的控制台视图。

视图只读并组器的公开查询接口，与停堆逻辑、审计流水读的是同一份状态；
``policy`` 段把并组窗口、首因策略与规则版本一并暴露，值班员看到的每组结论
都能对应到"是按哪套参数并的、按哪版规则标的"。
"""

from __future__ import annotations

from .correlator import AlarmCorrelator
from .state import CLEARED, Group


def _member_view(group: Group, correlator: AlarmCorrelator) -> list[dict]:
    label = group.label
    present = set(group.members)
    rows = []
    for member in sorted(group.members.values(), key=lambda m: (m.first_ts, m.first_seq)):
        if label is not None and member.alarm_id == label.root:
            role = "root"
        elif correlator.rules.caused_by_member(member.alarm_id, present):
            role = "caused"
        else:
            role = "member"
        rows.append(
            {
                "alarm_id": member.alarm_id,
                "channel": member.channel,
                "severity": member.severity,
                "occurrences": member.occurrences,
                "cleared": member.cleared,
                "momentary": member.momentary,
                "first_ts": round(member.first_ts, 3),
                "role": role,
            }
        )
    return rows


def _group_view(group: Group, correlator: AlarmCorrelator) -> dict:
    return {
        "group_id": group.group_id,
        "status": group.status,
        "opened_ts": round(group.opened_ts, 3),
        "last_active_ts": round(group.last_active_ts, 3),
        "cleared_ts": round(group.cleared_ts, 3) if group.status == CLEARED else None,
        "member_seq": group.member_seq,
        "clear_blocked": group.clear_blocked,
        "label": group.label.as_dict() if group.label is not None else None,
        "members": _member_view(group, correlator),
    }


def build_alarm_view(correlator: AlarmCorrelator) -> dict:
    """汇总控制台展示所需的并组全貌。"""

    basis = correlator.shutdown_basis()
    limits = correlator.limits
    return {
        "health": correlator.health(),
        "shutdown_basis": basis.as_dict() if basis is not None else None,
        "groups": [_group_view(group, correlator) for group in correlator.groups()],
        "policy": {
            "grouping": "time-window",
            "window_s": limits.window_s,
            "simultaneity_epsilon_s": limits.simultaneity_epsilon_s,
            "unclear_policy": limits.unclear_policy,
            "clearance_requires_full_health": limits.clearance_requires_full_health,
            "rule_version": correlator.rules.version,
        },
    }
