"""告警因果链并组、首因标注、缺采部分失败与整组解除。"""

from __future__ import annotations

from dataclasses import replace

import pytest

from heatnet.alarm.catalog import (
    CODES,
    direct_causes,
    is_root_code,
    root_ancestors,
)
from heatnet.alarm.correlator import AlarmCorrelator
from heatnet.alarm.group import (
    GROUP_CLOSED,
    GROUP_OPEN,
    GROUP_RECOVERED,
    ROOT_CONFIRMED,
    ROOT_INFERRED,
    ROOT_PENDING,
)
from heatnet.config import default_config


@pytest.fixture()
def correlator(clock) -> AlarmCorrelator:
    return AlarmCorrelator(default_config().alarm, clock=clock)


def test_causal_chain_is_fixed_in_catalog():
    assert direct_causes("primary.low_flow") == ("src.overtemp", "src.fuel_fault")
    assert "primary.low_flow" in direct_causes("station.low_supply")
    assert "valve.stuck" in direct_causes("station.low_supply")
    assert is_root_code("src.overtemp")
    assert not is_root_code("valve.forced_open")
    assert set(root_ancestors("secondary.low_pressure")) == {
        "pump.fault",
        "src.overtemp",
        "src.fuel_fault",
        "valve.stuck",
    }
    assert set(root_ancestors("valve.forced_open")) == {
        "src.overtemp",
        "src.fuel_fault",
        "valve.stuck",
    }
    assert CODES["src.overtemp"].trip is True
    assert CODES["station.low_supply"].trip is False


def test_cascade_merges_into_one_group_with_first_root(correlator, clock):
    correlator.report("s1", "src.overtemp")
    clock.advance(1)
    correlator.report("flow", "primary.low_flow")
    clock.advance(1)
    correlator.report("st01", "station.low_supply", scope="st01")
    clock.advance(1)
    correlator.report("st01.mix", "valve.forced_open", scope="st01")

    groups = correlator.groups()
    assert len(groups) == 1
    group = groups[0]
    assert group["root_code"] == "src.overtemp"
    assert group["root_seq"] == 1
    assert group["root_status"] == ROOT_CONFIRMED
    assert group["trip_qualified"] is True
    assert group["alarm_count"] == 4
    assert [item["seq"] for item in group["observations"]] == [1, 2, 3, 4]


def test_reverse_arrival_order_still_marks_true_root(correlator, clock):
    # 现场常常是下游先报、首因后到：并组与标注必须能把迟到的真首因认出来。
    correlator.report("st01", "station.low_supply", scope="st01")
    pending = correlator.groups()[0]
    assert pending["root_status"] == ROOT_PENDING
    assert "src.overtemp" in pending["expected_roots"]

    clock.advance(1)
    correlator.report("flow", "primary.low_flow")
    clock.advance(1)
    correlator.report("s1", "src.overtemp")

    groups = correlator.groups()
    assert len(groups) == 1
    assert groups[0]["root_code"] == "src.overtemp"
    assert groups[0]["root_status"] == ROOT_CONFIRMED
    assert groups[0]["root_seq"] == 3


def test_unrelated_alarms_in_same_window_do_not_merge(correlator, clock):
    # 时间窗再近，因果图不相连就不能并组——并组依据是因果链，不是时间窗。
    correlator.report("s1", "src.overtemp")
    clock.advance(1)
    correlator.report("pump", "pump.fault")
    clock.advance(1)
    correlator.report("st02.mix", "valve.stuck", scope="st02")

    groups = correlator.groups()
    assert len(groups) == 3
    assert {group["root_code"] for group in groups} == {
        "src.overtemp",
        "pump.fault",
        "valve.stuck",
    }


def test_scoped_causal_edges_do_not_cross_station(correlator, clock):
    correlator.report("st01.mix", "valve.stuck", scope="st01")
    clock.advance(1)
    correlator.report("st02", "station.low_supply", scope="st02")
    groups = correlator.groups()
    assert len(groups) == 2

    # 同站的卡涩与供温低则并成一组。
    clock.advance(1)
    correlator.report("st01", "station.low_supply", scope="st01")
    groups = correlator.groups()
    assert len(groups) == 2
    stuck_group = next(group for group in groups if group["root_code"] == "valve.stuck")
    assert stuck_group["alarm_count"] == 2


def test_channel_chatter_stays_in_original_group(correlator, clock):
    correlator.report("s1", "src.overtemp", message="第一次")
    clock.advance(1)
    correlator.report("s1", "src.overtemp", message="抖一下")
    clock.advance(1)
    correlator.report("s1", "src.overtemp", message="又抖一下")
    groups = correlator.groups()
    assert len(groups) == 1
    assert groups[0]["alarm_count"] == 3
    assert len(groups[0]["active_channels"]) == 1


def test_alarms_outside_causal_window_open_new_group(correlator, clock):
    correlator.report("s1", "src.overtemp")
    clock.advance(40)
    correlator.report("flow", "primary.low_flow")
    groups = correlator.groups()
    assert len(groups) == 2
    assert groups[1]["root_status"] == ROOT_PENDING


def test_pending_root_does_not_trip_but_hold_then_heaviest_infers(correlator, clock):
    correlator.report("st01", "station.low_supply", scope="st01")
    assert correlator.due_trips() == []

    clock.advance(10)
    moved = correlator.tick()
    assert moved["root_inferred"]
    group = correlator.groups()[0]
    assert group["root_status"] == ROOT_INFERRED
    assert group["root_code"] == "station.low_supply"
    # 推定的首因不是停机码，不能联锁停机。
    assert correlator.due_trips() == []


def test_inferred_heavy_trip_code_does_trip_once(correlator, clock):
    # 只有一条带停机资格的二次失压，首因一直没来；挂起到点按最重一条推定并停机。
    correlator.report("secondary", "secondary.low_pressure")
    clock.advance(10)
    correlator.tick()
    due = correlator.due_trips()
    assert len(due) == 1
    assert due[0]["root_code"] == "secondary.low_pressure"
    assert due[0]["root_status"] == ROOT_INFERRED
    assert correlator.due_trips() == []


def test_missing_upstream_is_partial_failure_and_blocks_recovery(correlator, clock):
    correlator.report("flow", "primary.low_flow")
    clock.advance(12)
    moved = correlator.tick()
    assert set(moved["missing"]) == {"src.fuel_fault", "src.overtemp"}

    group = correlator.groups()[0]
    assert any(item["code"] == "channel.missing" for item in group["observations"])

    # 现场流量恢复，缺采仍在：整组不能自动解除。
    clock.advance(1)
    correlator.report("flow", "primary.low_flow", cleared=True)
    correlator.tick()
    assert correlator.groups()[0]["state"] != GROUP_RECOVERED


def test_late_root_clears_missing_and_group_recovers(correlator, clock):
    correlator.report("flow", "primary.low_flow")
    clock.advance(12)
    correlator.tick()
    assert correlator.groups()[0]["missing_channels"]

    # 缺采判定后真首因补到（仍在因果窗内）：解除缺采标记，标注升级为确认。
    clock.advance(1)
    correlator.report("s1", "src.fuel_fault", ts=clock.now())
    group = correlator.groups()[0]
    assert group["missing_channels"] == []
    assert group["root_status"] == ROOT_CONFIRMED

    correlator.report("flow", "primary.low_flow", cleared=True)
    correlator.report("s1", "src.fuel_fault", cleared=True)
    correlator.tick()
    assert correlator.groups()[0]["state"] == GROUP_RECOVERED


def test_full_group_recovers_only_when_every_channel_clears(correlator, clock):
    correlator.report("s1", "src.overtemp")
    clock.advance(1)
    correlator.report("flow", "primary.low_flow")
    clock.advance(30)
    correlator.tick()
    assert correlator.groups()[0]["state"] == GROUP_CLOSED

    clock.advance(1)
    correlator.report("s1", "src.overtemp", cleared=True)
    correlator.tick()
    assert correlator.groups()[0]["state"] == GROUP_CLOSED

    clock.advance(1)
    correlator.report("flow", "primary.low_flow", cleared=True)
    correlator.tick()
    assert correlator.groups()[0]["state"] == GROUP_RECOVERED
    assert correlator.active_groups() == []


def test_group_closes_after_window_and_keeps_late_causal_alarm(correlator, clock):
    correlator.report("s1", "src.overtemp")
    # 闭窗时刻恰好在因果窗内补到一条果告警：先闭窗、后补录，状态不回退为开窗。
    clock.advance(8)
    correlator.report("flow", "primary.low_flow")
    clock.advance(7)
    moved = correlator.tick()
    assert moved["closed"]
    group = correlator.groups()[0]
    assert group["state"] == GROUP_CLOSED
    assert group["alarm_count"] == 2


def test_acknowledge_records_operator_but_does_not_fake_recovery(correlator, clock):
    correlator.report("flow", "primary.low_flow")
    clock.advance(12)
    correlator.tick()
    group_id = correlator.groups()[0]["group_id"]
    clock.advance(1)
    correlator.acknowledge(group_id, "shift-lead")
    group = correlator.groups()[0]
    assert group["acknowledged"] is True
    assert group["ack_operator"] == "shift-lead"
    assert group["state"] != GROUP_RECOVERED


def test_snapshot_restore_round_trip(correlator, clock):
    correlator.report("s1", "src.overtemp")
    clock.advance(1)
    correlator.report("flow", "primary.low_flow")
    correlator.tick()
    raw = correlator.snapshot()

    rebuilt = AlarmCorrelator(default_config().alarm, clock=clock)
    rebuilt.restore(raw)
    groups = rebuilt.groups()
    assert len(groups) == 1
    assert groups[0]["group_id"] == correlator.groups()[0]["group_id"]
    assert groups[0]["root_code"] == "src.overtemp"
    assert groups[0]["alarm_count"] == 2


def test_heaviest_immediate_policy_infers_without_hold(clock):
    config = default_config()
    config = replace(config, alarm=replace(config.alarm, unknown_root_policy="heaviest-immediate"))
    fast = AlarmCorrelator(config.alarm, clock=clock)
    fast.report("st01", "station.low_supply", scope="st01")
    group = fast.groups()[0]
    assert group["root_status"] == ROOT_INFERRED
    assert fast.groups()[0]["state"] == GROUP_OPEN


def test_unknown_code_is_pending_and_not_trip(correlator, clock):
    correlator.report("odd", "mystery.code")
    group = correlator.groups()[0]
    assert group["root_status"] == ROOT_PENDING
    clock.advance(10)
    correlator.tick()
    assert correlator.due_trips() == []


def test_events_publish_grouping_and_root_timeline(clock):
    from heatnet.eventbus import EventBus

    events: list[str] = []
    bus = EventBus()
    bus.subscribe("alarm", lambda event: events.append(event.action))
    correlator = AlarmCorrelator(default_config().alarm, clock=clock, bus=bus)

    correlator.report("flow", "primary.low_flow")
    clock.advance(1)
    correlator.report("s1", "src.overtemp")

    assert "alarm_group_opened" in events
    assert "alarm_attached" in events
    assert "alarm_root_marked" in events
    marked = [action for action in events if action == "alarm_root_marked"]
    assert len(marked) == 1
