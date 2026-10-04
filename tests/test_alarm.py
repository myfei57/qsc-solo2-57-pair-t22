"""告警并组与首因标注的行为验证。

覆盖需求里写死的几条：时间窗并组、首因全序（先出 / 因果 / 最重暂定 / 挂起）、
状态机时序、通道缺失按部分失败、解除确认、停堆依据与审计一致。
"""

from __future__ import annotations

import dataclasses

import pytest

from heatnet.alarm.state import SEVERITY_WEIGHT
from heatnet.app import build_platform
from heatnet.clock import ManualClock
from heatnet.config import default_config
from heatnet.console.api import Api
from heatnet.errors import AlarmError


def _records(platform, action):
    return [r for r in platform.audit.tail(1000) if r["action"] == action]


def _group(view, index=0):
    return view["groups"][index]


def test_jitter_folds_into_one_member(platform):
    """温度抖一下就是一条：同一告警反复报，只算一个成员，次数累加。"""

    for _ in range(5):
        platform.raise_alarm("source.overtemp", "warning", "source")
    view = platform.alarm_report()
    assert len(view["groups"]) == 1
    members = _group(view)["members"]
    assert len(members) == 1
    assert members[0]["occurrences"] == 5
    assert len(_records(platform, "member_added")) == 1
    assert len(_records(platform, "member_refired")) == 4


def test_burst_groups_and_first_out_root(platform, clock):
    """一口气蹦出的几条告警并成一组，首因判给最先报出的那条。"""

    platform.raise_alarm("A.one", "warning", "c1")
    clock.advance(2.0)
    platform.raise_alarm("B.two", "alarm", "c1")
    platform.raise_alarm("C.three", "warning", "c1")
    view = platform.alarm_report()
    assert len(view["groups"]) == 1
    group = _group(view)
    assert [m["alarm_id"] for m in group["members"]] == ["A.one", "B.two", "C.three"]
    label = group["label"]
    assert label["root"] == "A.one"
    assert label["confidence"] == "certain"
    assert label["reason"] == "first-out"
    assert view["shutdown_basis"]["group_id"] == group["group_id"]
    assert view["shutdown_basis"]["root"] == "A.one"


def test_causal_rule_overrides_report_order(platform, clock):
    """被带出来的告警先报，首因仍按因果规则判给源头。"""

    platform.raise_alarm("primary.low_flow", "warning", "primary")
    clock.advance(1.0)
    platform.raise_alarm("source.overtemp", "alarm", "source")
    view = platform.alarm_report()
    group = _group(view)
    assert group["label"]["root"] == "source.overtemp"
    assert group["label"]["reason"] == "sole-candidate"
    roles = {m["alarm_id"]: m["role"] for m in group["members"]}
    assert roles["source.overtemp"] == "root"
    assert roles["primary.low_flow"] == "caused"


def test_window_splits_groups(platform, clock):
    """并组按时间窗：窗口之外的告警另开新组，旧组收口冻结。"""

    platform.raise_alarm("A", "warning", "c1")
    clock.advance(31.0)
    platform.raise_alarm("B", "warning", "c1")
    view = platform.alarm_report()
    assert len(view["groups"]) == 2
    assert view["groups"][0]["status"] == "closed"
    assert view["groups"][1]["status"] == "open"
    assert view["policy"]["grouping"] == "time-window"


def test_unclear_root_report_worst_provisional(platform):
    """首因不明且系统配置 report_worst：按最重的那条报，标暂定，依据可执行。"""

    platform.raise_alarm("X.alpha", "warning", "c1")
    platform.raise_alarm("X.beta", "alarm", "c1")  # 同一时刻，分不出先后
    view = platform.alarm_report()
    label = _group(view)["label"]
    assert label["confidence"] == "provisional"
    assert label["root"] == "X.beta"
    assert label["reason"] == "root-ambiguous:worst-of-tie"
    basis = view["shutdown_basis"]
    assert basis["actionable"] is True
    assert basis["root_severity"] == "alarm"


def test_unclear_root_hold_suspends_basis(tmp_path):
    """首因不明且系统配置 hold：首因挂起，停堆依据不可执行，alarm 级也不动作。"""

    config = default_config()
    config = dataclasses.replace(
        config, alarm=dataclasses.replace(config.alarm, unclear_policy="hold")
    )
    platform = build_platform(config, clock=ManualClock(), storage_root=str(tmp_path / "state"))
    platform.raise_alarm("X.alpha", "warning", "c1")
    platform.raise_alarm("X.beta", "alarm", "c1")
    view = platform.alarm_report()
    label = _group(view)["label"]
    assert label["confidence"] == "pending"
    assert label["root"] is None
    basis = view["shutdown_basis"]
    assert basis["actionable"] is False
    platform.cycle(dt=1.0)
    assert platform.source.state() != "tripped"


def test_missing_channel_is_partial_failure_not_all_clear(platform, clock):
    """某路告警缺失是部分失败：系统转 partial，证据不全的组不得判解除。"""

    platform.raise_alarm("A", "warning", "ch1")
    platform.alarm_heartbeat("ch2")
    clock.advance(61.0)
    platform.alarms.tick()
    health = platform.alarm_report()["health"]
    assert health["status"] == "partial"
    assert set(health["degraded_channels"]) == {"ch1", "ch2"}

    platform.clear_alarm("A", "ch1")  # ch1 随清除恢复，ch2 仍缺失
    view = platform.alarm_report()
    group = _group(view)
    assert group["status"] != "cleared"
    assert group["clear_blocked"] is True
    assert len(_records(platform, "clear_blocked")) == 1
    # 不是"没有告警"：停堆依据仍然指向这个未解除的组
    assert view["shutdown_basis"] is not None
    assert view["shutdown_basis"]["group_id"] == group["group_id"]


def test_recovery_confirms_group_cleared(platform, clock):
    """通道恢复后整组确认解除，审计顺序：收口 → 阻塞 → 解除。"""

    platform.raise_alarm("A", "warning", "ch1")
    platform.alarm_heartbeat("ch2")
    clock.advance(61.0)
    platform.alarms.tick()
    platform.clear_alarm("A", "ch1")
    platform.alarm_heartbeat("ch2")
    view = platform.alarm_report()
    assert _group(view)["status"] == "cleared"
    assert view["health"]["status"] == "ok"
    assert view["shutdown_basis"] is None

    actions = [r["action"] for r in platform.audit.tail(1000)]
    assert actions.index("group_closed") < actions.index("clear_blocked")
    assert actions.index("clear_blocked") < actions.index("group_cleared")
    assert "channel_degraded" in actions
    assert actions.index("channel_degraded") < actions.index("channel_recovered")


def test_trip_carries_shutdown_basis(platform):
    """停堆只认首因那一组：停机审计携带依据组、首因与成员摘要，三方一致。"""

    platform.raise_alarm("source.overtemp", "alarm", "source")
    result = platform.trip("manual-check")
    assert result["basis"]["group_id"] == "AG0001"
    assert result["basis"]["root"] == "source.overtemp"

    trips = [r for r in platform.audit.tail(1000) if r["action"] == "trip"]
    payload = trips[-1]["payload"]
    assert payload["basis_group"] == "AG0001"
    assert payload["basis_root"] == "source.overtemp"

    labeled = _records(platform, "root_labeled")[-1]["payload"]
    assert labeled["root"] == payload["basis_root"]
    assert labeled["members_digest"] == payload["basis_digest"]
    assert labeled["member_seq"] == payload["basis_member_seq"]


def test_basis_and_label_share_digest(platform, clock):
    """停堆依据与首因标注基于同一版成员集：member_seq 与摘要一致。"""

    platform.raise_alarm("A.one", "warning", "c1")
    clock.advance(2.0)
    platform.raise_alarm("B.two", "alarm", "c1")
    labeled = _records(platform, "root_labeled")[-1]["payload"]
    basis_events = _records(platform, "shutdown_basis")[-1]["payload"]
    assert basis_events["group_id"] == labeled["group"]
    assert basis_events["member_seq"] == labeled["member_seq"]
    assert basis_events["members_digest"] == labeled["members_digest"]
    view = platform.alarm_report()
    assert view["shutdown_basis"]["members_digest"] == labeled["members_digest"]


def test_auto_trip_only_on_actionable_alarm_root(platform):
    """alarm 级首因且依据可执行时，控制周期末尾自动停堆并引用首因组。"""

    platform.raise_alarm("source.overtemp", "alarm", "source")
    platform.cycle(dt=1.0)
    assert platform.source.state() == "tripped"
    trips = [r for r in platform.audit.tail(1000) if r["action"] == "trip"]
    assert trips[-1]["payload"]["reason"] == "alarm-group:AG0001:source.overtemp"


def test_no_auto_trip_on_warning_root(platform):
    """warning 级首因只并组标注，不触发停堆。"""

    platform.raise_alarm("source.overtemp", "warning", "source")
    platform.cycle(dt=1.0)
    assert platform.source.state() != "tripped"


def test_out_of_order_ingest_rejected(platform, clock):
    """时序不能乱：时钟回退后的摄入直接拒绝。"""

    platform.raise_alarm("A", "warning", "c1")
    clock._now -= 5.0  # 模拟时钟回退故障
    with pytest.raises(AlarmError):
        platform.raise_alarm("B", "warning", "c1")


def test_orphan_clear_is_audited(platform):
    """收到没有对应在报告警的解除，按孤儿解除落审计，不悄悄吞掉。"""

    platform.clear_alarm("ghost.alarm", "c1")
    orphans = _records(platform, "orphan_clear")
    assert len(orphans) == 1
    assert orphans[0]["payload"]["alarm"] == "ghost.alarm"
    assert orphans[0]["severity"] == "warning"


def test_clearance_needs_all_members(platform, clock):
    """整组解除以全部成员清除为前提，少一条都不判解除。"""

    platform.raise_alarm("A", "warning", "c1")
    platform.raise_alarm("B", "warning", "c1")
    platform.clear_alarm("A", "c1")
    clock.advance(31.0)
    platform.alarms.tick()
    view = platform.alarm_report()
    assert _group(view)["status"] == "closed"
    platform.clear_alarm("B", "c1")
    view = platform.alarm_report()
    assert _group(view)["status"] == "cleared"


def test_momentary_alarm_does_not_block_clearance(platform, clock):
    """点告警（联锁拒绝类）入组即视为已解除，不阻塞整组解除。"""

    platform.raise_alarm("interlock.rejected", "alarm", "plant", momentary=True)
    clock.advance(31.0)
    platform.alarms.tick()
    view = platform.alarm_report()
    assert _group(view)["status"] == "cleared"


def test_severity_weight_table_covers_audit_levels():
    assert set(SEVERITY_WEIGHT) == {"notice", "warning", "alarm"}


def test_console_routes(platform):
    """并组结果供控制台展示：查询与动作接口都走平台同一份状态。"""

    api = Api(platform)
    status, payload = api.handle("POST", "/api/alarm/raise", {}, {"alarm_id": "A", "severity": "warning", "channel": "c1"})
    assert status == 200
    assert payload["basis"]["group_id"] == "AG0001"

    status, payload = api.handle("GET", "/api/alarm/groups", {}, {})
    assert status == 200
    assert len(payload["groups"]) == 1
    assert payload["health"]["status"] == "ok"
    assert payload["policy"]["unclear_policy"] == "report_worst"

    status, _ = api.handle("POST", "/api/alarm/heartbeat", {}, {"channel": "c1"})
    assert status == 200
    status, payload = api.handle("POST", "/api/alarm/clear", {}, {"alarm_id": "A", "channel": "c1"})
    assert status == 200

    status, payload = api.handle("POST", "/api/alarm/raise", {}, {"alarm_id": "", "severity": "warning"})
    assert status == 400
    status, payload = api.handle("POST", "/api/alarm/raise", {}, {"alarm_id": "B", "severity": "fatal"})
    assert status == 400
