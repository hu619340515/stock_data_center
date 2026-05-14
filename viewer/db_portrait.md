# 数据库画像（DB Portrait）

> 本文档旨在帮助开发者快速理解 viewer 工具所用数据库的结构与数据分布。

---

## 数据库文件路径
- 默认路径：`./data/data.db`（可在 server.py 中配置）

## 表结构总览

| 表名              | 说明             | 行数             | 主要字段                |
|-------------------|------------------|------------------|-------------------------|
| stocks            | 股票列表         | 运行时查询       | symbol, name, market, industry, area, list_date |
| daily_prices      | 日线行情         | 运行时查询       | symbol, trade_date, open, close, high, low, volume, amount |
| minute_prices     | 分钟线行情       | 运行时查询       | symbol, datetime, open, close, high, low, volume, amount |
| financials        | 财务数据         | 运行时查询       | symbol, report_date, revenue, profit, assets, liabilities |
| events            | 重要事件         | 运行时查询       | symbol, event_date, event_type, description |
| users             | 用户信息         | 运行时查询       | user_id, username, role, created_at |

---

## 各表字段详情

### 1. stocks — 股票列表
- symbol：股票代码（主键，字符串）
- name：股票简称
- market：市场板块（如上交所/深交所/科创板等）
- industry：所属行业
- area：地域
- list_date：上市日期

#### 数据范围
- symbol: 如 '600519', '000001'
- list_date: 2000-01-01 ~ 当前日期

#### 索引建议
- 主键：symbol
- 可加 market, industry 联合索引用于筛选

---

### 2. daily_prices — 日线行情
- symbol：股票代码（外键）
- trade_date：交易日期（主键之一）
- open：开盘价
- close：收盘价
- high：最高价
- low：最低价
- volume：成交量
- amount：成交额

#### 数据范围
- trade_date: 2000-01-01 ~ 当前日期
- 数值型字段均为浮点数

#### 索引建议
- 联合主键：symbol + trade_date
- 可加 trade_date 单独索引用于区间筛选

---

### 3. minute_prices — 分钟线行情
- symbol：股票代码（外键）
- datetime：分钟时间（主键之一，格式 'YYYY-MM-DD HH:MM'）
- open, close, high, low, volume, amount：同上

#### 数据范围
- datetime: 近一年（或按存储容量调整）

#### 索引建议
- 联合主键：symbol + datetime
- 可加 datetime 索引优化区间查询

---

### 4. financials — 财务数据
- symbol：股票代码（外键）
- report_date：报告期（主键之一）
- revenue：营业收入
- profit：净利润
- assets：总资产
- liabilities：总负债

#### 数据范围
- report_date: 通常为每季度末（如 2023-03-31）

#### 索引建议
- 联合主键：symbol + report_date
- 可加 revenue, profit 索引便于筛选排序

---

### 5. events — 重要事件
- symbol：股票代码（外键）
- event_date：事件时间（主键之一）
- event_type：事件类型（如分红、重组等）
- description：事件描述

#### 数据范围
- event_type: 『分红』、『重组』、『业绩快报』等

#### 索引建议
- 联合主键：symbol + event_date + event_type

---

### 6. users — 用户信息
- user_id：用户唯一标识（主键，整数）
- username：用户名
- role：用户角色（如 admin、viewer）
- created_at：注册时间

#### 索引建议
- 主键：user_id
- 可加 username 唯一索引

---

## 其他说明
- 所有表均支持分页、高级筛选，建议为主筛维度加索引。
- DuckDB 支持多表高效 JOIN 查询，复杂分析可直接写 SQL。
- 如需扩展字段/表结构，可在 server.py 的 INIT SQL 部分调整。

---

> 如需了解更多，参考 `viewer/server.py` 中的建表/初始化逻辑。
