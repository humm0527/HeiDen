# Broker mapping versions

每个 `broker_mapping_vN.yaml` 是一个完整、不可变的映射版本，必须包含：

- `mapping_version`
- `change_control.change_reason`
- `change_control.previous`
- `change_control.new`
- `change_control.effective_date`
- `change_control.approved_by`
- `change_control.supersedes`

映射变化必须新增下一版本，禁止原地覆盖已生效版本。历史任务必须记录实际使用的映射版本。

- `v1`：只包含完全虚构的券商和代码，未批准生效，保留用于历史合成接入测试。
- `v2`：已批准的集中度分类映射。对宽表中已声明枚举的 15 家券商逐原值列举，同时保留 v1 合成映射用于黄金回归。其余 6 家因尚无非空枚举而不猜测；出现非空值时记 `UNKNOWN`、排除该记录指标并审计。

最终分类还必须叠加 D-051 的每日 ST 优先级；`broker_mapping_v2.yaml` 不允许单独绕过 ST 状态缺失、ST 覆盖或空白分类规则。
