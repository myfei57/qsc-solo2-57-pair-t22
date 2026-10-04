"""告警并组与首因标注。

现场告警一条一条地报，温度抖一下就是一条，真出事时一口气蹦出七八条，值班员
分不清哪条是首因、哪些是被带出来的。本包把时间上挨着的告警并成一组、标出
首因、给出唯一的停堆依据，并把每一步结论发回事件总线落审计：

- 并组按**时间窗**（写死，不按因果链），因果规则只用于组内首因排序；
- 首因不明时按系统配置处置：``report_worst`` 报最重的一条（标暂定）或
  ``hold`` 挂起等首因明确；
- 组状态 ``open → closed → cleared`` 单调前进，摄入时序回退直接拒绝；
- 通道心跳缺失按**部分失败**处理：系统转入 ``partial``，证据不全的组不得
  判解除，绝不能当成没有告警；
- 停堆只认首因那一组：:meth:`AlarmCorrelator.shutdown_basis` 是唯一入口，
  依据携带组号、首因、成员版本与成员摘要，与审计逐条对得上。
"""

from __future__ import annotations

from .correlator import AlarmCorrelator, compute_label, members_digest
from .report import build_alarm_view
from .rules import CausalRule, RuleSet, default_rules
from .state import RootCauseLabel, ShutdownBasis

__all__ = [
    "AlarmCorrelator",
    "CausalRule",
    "RootCauseLabel",
    "RuleSet",
    "ShutdownBasis",
    "build_alarm_view",
    "compute_label",
    "default_rules",
    "members_digest",
]
