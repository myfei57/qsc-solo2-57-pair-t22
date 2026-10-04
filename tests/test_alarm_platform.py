"""告警并组在平台与控制台层面的端到端口径。

覆盖：并组结果供控制台展示并落审计、联锁停机只认首因组、非首因组不停机、
整组恢复可见、某路缺失按部分失败阻塞恢复、并组依据/停机依据/审计三处一致、
重启后并组与停机态一起恢复。
"""

from __future__ import annotations

from heatnet.console.api import Api
from heatnet.app import build_platform
from heatnet.config import default_config


def _audit_actions(platform) -> list[str]:
    return [entry["action"] for entry in platform.audit.entries(limit=400)]


def test_platform_groups_cascade_and_trips_on_root_only(online, clock):
    # 下游先报、首因后到，整链并成一组。
    online.raise_alarm("st01.sensor", "station.low_supply", scope="st01")
    clock.advance(1)
    online.raise_alarm("primary.flow_meter", "primary.low_flow")
    assert online.source.state() != "tripped"  # 首因未明，挂起中不能停机

    clock.advance(1)
    result = online.raise_alarm("plant.heat_source", "src.overtemp")
    assert online.source.state() == "tripped"
    assert result["auto_trips"][0]["root_code"] == "src.overtemp"

    groups = online.alarm_groups()
    assert len(groups) == 1
    group = groups[0]
    assert group["policy"] == "causal-chain"
    assert group["root_code"] == "src.overtemp"
    assert group["root_status"] == "confirmed"
    assert group["tripped"] is True

    report = online.alarm_report()
    assert report["tripped_group"]["group_id"] == group["group_id"]


def test_non_root_group_does_not_trip(online, clock):
    # 只有下游非停机码告警：挂起到点按最重推定，但首因码不具停机资格，热源不停。
    online.raise_alarm("st01.sensor", "station.low_supply", scope="st01")
    clock.advance(11)
    moved = online.alarm_tick()
    assert moved["root_inferred"]
    assert online.source.state() != "tripped"
    assert online.alarm_groups()[0]["root_code"] == "station.low_supply"
    assert online.alarm_groups()[0]["trip_qualified"] is False


def test_separate_causal_chains_stay_separate(online, clock):
    online.raise_alarm("plant.heat_source", "src.overtemp")
    clock.advance(1)
    online.raise_alarm("secondary.circulating", "pump.fault")
    clock.advance(1)
    online.raise_alarm("st03.mix", "valve.stuck", scope="st03")
    assert len(online.alarm_groups()) == 3


def test_group_recovery_is_visible_after_all_clear(online, clock):
    online.raise_alarm("plant.heat_source", "src.overtemp")
    clock.advance(1)
    online.raise_alarm("primary.flow_meter", "primary.low_flow")
    clock.advance(30)
    online.alarm_tick()
    assert online.alarm_groups(active_only=True)

    online.clear_alarm("plant.heat_source", "src.overtemp")
    online.clear_alarm("primary.flow_meter", "primary.low_flow")
    clock.advance(1)
    online.alarm_tick()

    assert online.alarm_groups(active_only=True) == []
    group = online.alarm_groups()[0]
    assert group["state"] == "recovered"
    assert group["recovered_ts"] is not None
    actions = _audit_actions(online)
    assert "alarm_group_recovered" in actions


def test_missing_channel_is_partial_failure_and_audited(online, clock):
    online.raise_alarm("primary.flow_meter", "primary.low_flow")
    clock.advance(12)
    moved = online.alarm_tick()
    assert moved["missing"]

    group = online.alarm_groups()[0]
    assert group["missing_channels"]
    # 缺采合成的是真实告警档记录，不是"没有告警"。
    assert any(item["code"] == "channel.missing" for item in group["observations"])
    actions = _audit_actions(online)
    assert "alarm_channel_missing" in actions

    # 即使现场流量恢复，缺采未解前整组不能解除。
    online.clear_alarm("primary.flow_meter", "primary.low_flow")
    clock.advance(1)
    online.alarm_tick()
    assert online.alarm_groups()[0]["state"] != "recovered"

    # 值班员确认被缺采阻塞的组。
    ack = online.acknowledge_alarm_group(group["group_id"], "shift-lead")
    assert ack["acknowledged"] is True
    assert "alarm_group_acknowledged" in _audit_actions(online)


def test_grouping_and_trip_basis_are_consistent_with_audit(online, clock):
    online.raise_alarm("plant.heat_source", "src.overtemp", message="超温")
    clock.advance(1)
    online.raise_alarm("primary.flow_meter", "primary.low_flow")
    clock.advance(2)
    online.alarm_tick()

    group = online.alarm_groups()[0]
    entries = {
        entry["action"]: entry
        for entry in platform_audit_alarm_entries(online)
    }
    assert "alarm_root_marked" in entries
    marked = entries["alarm_root_marked"]
    assert marked["payload"]["group_id"] == group["group_id"]
    assert marked["payload"]["root_code"] == "src.overtemp"

    auto = entries.get("auto_trip")
    assert auto is not None
    assert auto["payload"]["group_id"] == group["group_id"]
    assert auto["payload"]["root_seq"] == group["root_seq"]
    assert auto["payload"]["policy"] == "causal-chain"


def platform_audit_alarm_entries(platform):
    return [entry for entry in platform.audit.entries(limit=400) if entry["component"] == "alarm"]


def test_alarm_state_survives_restart(online, clock, tmp_path):
    online.raise_alarm("plant.heat_source", "src.overtemp")
    clock.advance(1)
    online.raise_alarm("primary.flow_meter", "primary.low_flow")
    clock.advance(1)
    online.alarm_tick()
    group_id = online.alarm_groups()[0]["group_id"]
    assert online.source.state() == "tripped"

    restarted = build_platform(default_config(), clock=clock, storage_root=online.store.root)
    groups = restarted.alarm_groups()
    assert len(groups) == 1
    assert groups[0]["group_id"] == group_id
    assert groups[0]["root_code"] == "src.overtemp"
    assert groups[0]["tripped"] is True
    assert restarted.source.state() == "tripped"


def test_console_exposes_groups_and_lifecycle(online, clock):
    api = Api(online)
    online.raise_alarm("plant.heat_source", "src.overtemp")
    clock.advance(1)
    online.raise_alarm("primary.flow_meter", "primary.low_flow")

    status, payload = api.handle("GET", "/api/alarm/groups", {"active": "true"}, {})
    assert status == 200
    assert payload["policy"] == "causal-chain"
    assert len(payload["groups"]) == 1
    assert payload["groups"][0]["root_code"] == "src.overtemp"

    group_id = payload["groups"][0]["group_id"]
    status, detail = api.handle("GET", f"/api/alarm/group/{group_id}", {}, {})
    assert status == 200
    assert detail["group"]["group_id"] == group_id

    # 再来一条同因果链上的被带告警，续进同一组（不是新的停机根因）。
    status, raised = api.handle(
        "POST",
        "/api/alarm/raise",
        {},
        {"channel": "st01", "code": "station.low_supply", "scope": "st01"},
    )
    assert status == 200
    assert raised["ok"] is True

    status, alarms = api.handle("GET", "/api/alarms", {}, {})
    assert status == 200
    assert alarms["tripped_group"]["root_code"] == "src.overtemp"
    assert len(alarms["groups"]) == 1


def test_console_clear_and_tick_recover_group(online, clock):
    api = Api(online)
    api.handle("POST", "/api/alarm/raise", {}, {"channel": "plant.heat_source", "code": "src.overtemp"})
    group_id = online.alarm_groups()[0]["group_id"]
    clock.advance(30)
    online.alarm_tick()
    status, cleared = api.handle(
        "POST", "/api/alarm/clear", {}, {"channel": "plant.heat_source", "code": "src.overtemp"}
    )
    assert status == 200
    assert cleared["ok"] is True
    # 最后一条告警恢复后在同一拍整组解除。
    assert online.alarm_group(group_id)["state"] == "recovered"
    status, ticked = api.handle("POST", "/api/alarm/tick", {}, {})
    assert status == 200
    assert ticked["recovered"] == []
    assert online.alarm_groups(active_only=True) == []


def test_console_rejects_unknown_group(online):
    api = Api(online)
    status, payload = api.handle("GET", "/api/alarm/group/nope", {}, {})
    assert status == 404
    assert payload["error"] == "not_found"


def test_manual_trip_attaches_to_alarm_group(online):
    result = online.trip("operator-overtemp")
    assert result["state"] == "tripped"
    assert result["group_id"]
    group = online.alarm_group(result["group_id"])
    assert group["root_code"] == "src.overtemp"
    assert group["tripped"] is True
