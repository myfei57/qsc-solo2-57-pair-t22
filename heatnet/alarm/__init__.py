"""告警并组子系统。

把时间上挨着、因果上相连的告警并成一组，按因果链标出首因，联锁停机只认首因组，
恢复后能看出整组确实解除。并组依据定死为因果链（见 :mod:`heatnet.alarm.catalog`）。
"""

from __future__ import annotations

from .catalog import (
    CODES,
    ROOT_CODES,
    AlarmCode,
    direct_causes,
    downstream_chain,
    get_code,
    is_known,
    is_root_code,
    root_ancestors,
)
from .correlator import AlarmCorrelator, code_label
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

__all__ = [
    "AlarmCode",
    "AlarmCorrelator",
    "AlarmGroup",
    "AlarmObservation",
    "CODES",
    "ROOT_CODES",
    "GROUP_OPEN",
    "GROUP_CLOSED",
    "GROUP_RECOVERED",
    "ROOT_PENDING",
    "ROOT_CONFIRMED",
    "ROOT_INFERRED",
    "code_label",
    "direct_causes",
    "downstream_chain",
    "get_code",
    "is_known",
    "is_root_code",
    "root_ancestors",
]
