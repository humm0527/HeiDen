# 版本化数据预检包

Validator 的 Python 引擎不绑定 RiskAudit 的五张表。每个业务通过一个版本化 YAML 校验包声明“检查哪些表、字段和跨表关系”，运行时使用 `--validation-pack` 选择。

内置校验包：

- `market_data_v1`：四张市场标准表；
- `broker_classification_v1`：第五张券商业务表；
- `riskaudit_full_v1`：五张表联合预检；
- `broker_wide_input_v1`：多券商横向分类原始宽表，内联声明各券商枚举，并检查周末日期、市场范围和单行填充数量。

## 新业务接入

新业务通常只需新增 YAML，不必修改 Validator：

```yaml
schema_version: 1
pack_id: customer_order
version: v1
description: 客户订单预检
tables:
  客户订单表:
    required: true
    primary_key: [订单编号]
    business_date_field: 订单日期
    fields:
      - {name: 订单编号, data_type: string, required: true, nullable: false}
      - {name: 订单日期, data_type: date, required: true, nullable: false}
      - {name: 金额, data_type: decimal, required: true, nullable: false}
      - {name: 订单状态, data_type: string, required: true, nullable: false}
    enums:
      订单状态: [待处理, 已完成]
```

如果表已经登记在 `configs/mapping/business_field_registry_v1.yaml`，可使用 `registry_table: 表名` 复用字段契约；否则使用 `fields` 内联声明，因此校验引擎可用于与证券无关的业务。

## 表级配置

- `required`：缺表或空表是否阻断；
- `primary_key`：单字段或组合主键；
- `business_date_field`：观察范围检查使用的业务日期；
- `security_code_field`：启用证券代码格式检查；
- `pit_fields`：声明源信息可得时间或证据截止时间；下载时间、入库时间和配置生效时间不得放入；
- `effective_date_rules`：声明“生效日期不得晚于使用日期”的字段对；
- `semantic_checks.options.delisted_date_exclusive`：为 `true` 时，终止上市日期当天起不再要求行情覆盖；
- `enums`：字段允许值；
- `fields`：字段类型、必需性和可空性。

字段类型支持 `string`、`integer`、`decimal`、`boolean`、`date` 和 `datetime`。

## 跨表语义检查

`semantic_checks` 用于需要多张表共同判断的确定性关系。当前显式注册：

- `market_calendar_coverage`：交易日、市场和应有证券覆盖；
- `security_lifecycle_consistency`：证券主数据、市场、上市前及退市后一致性；
- `broker_mapping_consistency`：券商原始分类、版本化映射及标准档位一致性。
- `wide_broker_classification_quality`：券商分类宽表的业务日、证券市场范围和单行填充数量检查。

YAML 只能引用已注册检查器，不能动态导入任意 Python。确有新型跨表业务关系时，新增一个确定性检查器、注册名称并配套测试；Agent 不进入正式判定链。

## 运行示例

```powershell
python scripts/run_data_validation.py --input-dir <标准表目录> --start-date 2026-01-01 --end-date 2026-01-31 --validation-pack riskaudit_full_v1
```

也可将 YAML 文件路径直接传给 `--validation-pack`。报告会记录实际使用的 `pack_id` 和 `version`，便于审计重放。
