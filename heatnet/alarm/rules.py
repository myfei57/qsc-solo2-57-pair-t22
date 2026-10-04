"""因果规则集（带版本）。

规则只参与组内首因排序，不参与并组成员归属——归属由时间窗唯一决定。这样因果
模型不全时最坏的结果是首因标不准，绝不会把本该同组的告警拆散。规则集带版本号，
首因标注与停堆依据都携带它，审计回放时能用同一版规则复算同一张标注。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CausalRule:
    """一条因果边：``parent`` 出现时 ``child`` 视为被带出来的告警。"""

    parent: str
    child: str


@dataclass(frozen=True)
class RuleSet:
    """一组带版本号的因果规则。"""

    version: str
    rules: frozenset

    def caused_by_member(self, alarm_id: str, present: set) -> bool:
        """``alarm_id`` 是否被某个同在组内的告警因果导致。"""

        return any(rule.child == alarm_id and rule.parent in present for rule in self.rules)


def default_rules() -> RuleSet:
    """平台内置因果规则：热源超温与泵停是被公认的告警源头。"""

    return RuleSet(
        "heatnet-1.0",
        frozenset(
            {
                CausalRule("source.overtemp", "primary.low_flow"),
                CausalRule("source.overtemp", "secondary.overpressure"),
                CausalRule("pump.trip", "primary.low_flow"),
            }
        ),
    )
