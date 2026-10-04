"""告警编码与因果链。

并组依据在这里定死：**只按因果链并组，不按纯时间窗并组**。时间窗
（``causal_window_s``）只用于两件事——判断两条有因果关系的告警是否属于同一次
扰动，以及闭窗封组；时间上挨着但因果图里不相连的告警绝不并到一起。

每条 :class:`AlarmCode` 声明：

* ``severity``：严重度权重（``notice``/``warning``/``alarm``，值越大越重）；
* ``trip``：首因达到该码时是否具备联锁停机资格（停堆只认首因组的首因码）；
* ``causes``：直接原因码。新告警先在所有"开着的组"里找"它的直接原因是否已在组内
  且时间差落在因果窗内"，找到即并入；这是唯一的并组入口。
* ``component`` / ``scoped``：该码挂在哪类设备上。``scoped=True`` 表示按站复制，
  因果传播不能跨站串线（st01 的阀卡涩不会并到 st02）。

链路依据与调度链拓扑一致：热源 → 一次网 → 换热站 → 阀门/泵 → 二次定压。
"""

from __future__ import annotations

from dataclasses import dataclass

SEVERITY_WEIGHT = {"notice": 0, "warning": 1, "alarm": 2}


@dataclass(frozen=True)
class AlarmCode:
    """一类告警：严重度、停机资格、直接原因与设备归属。"""

    code: str
    label: str
    severity: str
    component: str
    causes: tuple[str, ...] = ()
    scoped: bool = False
    trip: bool = False

    def weight(self) -> int:
        """严重度权重，供首因不明时挑最重一条。"""

        return SEVERITY_WEIGHT[self.severity]

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "label": self.label,
            "severity": self.severity,
            "component": self.component,
            "causes": list(self.causes),
            "scoped": self.scoped,
            "trip": self.trip,
        }


# 热源侧：超温是首因，能直接导致停机；联锁拒绝属于人工/顺序问题，不是超温的果。
SRC_OVERTEMP = AlarmCode(
    "src.overtemp", "热源供水超温", "alarm", "source", trip=True
)
SRC_FUEL_FAULT = AlarmCode(
    "src.fuel_fault", "热源燃烧/燃料故障", "alarm", "source", trip=True
)
SRC_INTERLOCK_REJECT = AlarmCode(
    "src.interlock_reject", "热源联锁拒绝", "alarm", "source"
)

# 一次网：低流量由热源波动/燃料故障带出，低流量会引起站点供温低。
PRIMARY_LOW_FLOW = AlarmCode(
    "primary.low_flow",
    "一次网低流量",
    "alarm",
    "primary",
    causes=("src.overtemp", "src.fuel_fault"),
)

# 站点：供温低由一次网低流量或本站混水阀卡涩带出；供温低又导致混水阀强开与
# 二次网降温。
STATION_LOW_SUPPLY = AlarmCode(
    "station.low_supply",
    "换热站二次供温低",
    "warning",
    "station",
    causes=("primary.low_flow", "valve.stuck"),
    scoped=True,
)

# 执行机构：阀卡涩是独立首因（机械问题，不是温度带出来的），会导致本站供温低；
# 阀强开是供温低的果，不能反过来当首因。
VALVE_STUCK = AlarmCode(
    "valve.stuck", "混水阀卡涩", "alarm", "valve", scoped=True, trip=True
)
VALVE_FORCED_OPEN = AlarmCode(
    "valve.forced_open",
    "混水阀被强制开大",
    "warning",
    "valve",
    causes=("station.low_supply",),
    scoped=True,
)

# 泵与二次网：泵故障是独立首因；泵故障与站点供温低都会导致二次网回水降温与失压。
PUMP_FAULT = AlarmCode(
    "pump.fault", "循环泵故障", "alarm", "pump", trip=True
)
SECONDARY_LOW_RETURN = AlarmCode(
    "secondary.low_return",
    "二次网回水温度低",
    "warning",
    "secondary",
    causes=("station.low_supply", "pump.fault"),
)
SECONDARY_LOW_PRESSURE = AlarmCode(
    "secondary.low_pressure",
    "二次网失压",
    "alarm",
    "secondary",
    causes=("pump.fault", "station.low_supply"),
    trip=True,
)

# 通信缺采：某一路告警该来没来，由并组器在缺采门限到时合成，属于部分失败，
# 绝不能当成"没有告警"。
CHANNEL_MISSING = AlarmCode(
    "channel.missing", "告警通道缺采", "alarm", "alarm"
)

ALL_CODES: tuple[AlarmCode, ...] = (
    SRC_OVERTEMP,
    SRC_FUEL_FAULT,
    SRC_INTERLOCK_REJECT,
    PRIMARY_LOW_FLOW,
    STATION_LOW_SUPPLY,
    VALVE_STUCK,
    VALVE_FORCED_OPEN,
    PUMP_FAULT,
    SECONDARY_LOW_RETURN,
    SECONDARY_LOW_PRESSURE,
    CHANNEL_MISSING,
)

CODES: dict[str, AlarmCode] = {item.code: item for item in ALL_CODES}

# 根因码：没有声明任何直接原因的真实告警码（channel.missing 是合成缺采，不算根因）。
ROOT_CODES: frozenset[str] = frozenset(
    item.code for item in ALL_CODES if not item.causes and item.code != CHANNEL_MISSING.code
)


def get_code(code: str) -> AlarmCode:
    """按编码取告警定义，未知编码按 :class:`KeyError` 抛出（调用方决定挂起）。"""

    return CODES[code]


def is_known(code: str) -> bool:
    """编码是否在因果表内。"""

    return code in CODES


def is_root_code(code: str) -> bool:
    """该码是否为没有上游原因的根因码。"""

    return code in ROOT_CODES


def direct_causes(code: str) -> tuple[str, ...]:
    """返回某码的直接原因码（因果表中查不到时为空）。"""

    entry = CODES.get(code)
    return entry.causes if entry is not None else ()


def root_ancestors(code: str) -> tuple[str, ...]:
    """沿因果链上溯，返回该码可能的全部根因码（传递闭包，按编码排序）。"""

    roots: set[str] = set()
    frontier = [code]
    seen: set[str] = set()
    while frontier:
        current = frontier.pop()
        if current in seen:
            continue
        seen.add(current)
        causes = direct_causes(current)
        if not causes:
            if is_root_code(current):
                roots.add(current)
            continue
        frontier.extend(causes)
    return tuple(sorted(roots))


def downstream_chain(code: str) -> tuple[str, ...]:
    """沿因果链下行，返回该码可能带出的全部果码（含自身，按编码排序）。"""

    found = {code}
    frontier = [code]
    while frontier:
        current = frontier.pop()
        for entry in ALL_CODES:
            if current in entry.causes and entry.code not in found:
                found.add(entry.code)
                frontier.append(entry.code)
    return tuple(sorted(found))
