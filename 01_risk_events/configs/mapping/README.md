# 字段映射配置

`business_field_registry_v1.yaml` 用于冻结五张标准业务表的中文字段名称，以及与其一一对应的稳定 Python 字段名称。

不同数据源的映射配置负责描述：

- 原始字段的精确名称；
- 已明确配置的字段别名；
- 已明确配置的候选字段；
- 调用方提供的上下文字段；
- 配置中明确指定的常量。

字段解析顺序固定为：

1. 精确字段；
2. 唯一存在的已配置别名；
3. 唯一存在的已配置候选字段；
4. 明确提供的上下文值；
5. 明确配置的常量。

如果多个候选字段同时存在，系统停止该字段的映射并返回“需要人工确认”。未知原始字段继续保留在原始数据层并记录，不自动猜测其业务含义。

市场事实数据映射与业务输入映射分别维护：

- `rqdata_v1.yaml`：`source_layer=market_data`，只映射四张市场标准表；
- `broker_json_v1.yaml`：`source_layer=business_input`，映射券商JSON输入；
- `broker_file_v1.yaml`：`source_layer=business_input`，映射券商CSV/Excel四字段输入。

`rqdata_v1.yaml` 基于米筐文档中的以下接口设计：

- `get_trading_dates`
- `all_instruments`
- `get_price`
- `is_suspended`
- `is_st_stock`

本项目不把券商风险分类虚构为米筐数据。券商分类通过 `broker_json_v1.yaml` 或 `broker_file_v1.yaml` 接入。
