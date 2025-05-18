# CPI计算器项目逻辑梳理

## 整体架构

本项目基于电商平台公开数据构建日频消费者价格指数，采用云服务进行数据管理与计算，实现高效、可复用的指数计算系统。整体架构分为三个核心模块：

```
数据导入(从天池到oss)->外部表映射(clickhouse访问oss)->指数计算(链式)─>可视化输出(QuickBI/本地)
```

## 核心模块解析

### 1. 配置管理 (config.py)

- 使用Dynaconf实现多环境配置管理
- 支持环境变量覆盖（前缀`CPI_`）
- 配置层级：
  ```yaml
  default: 基础配置
  ├── prod: 生产环境
  └── dev: 开发环境
  ```

### 2. 数据加载 (loader.py)

- 安全地连接 OSS（对象存储服务），通过 STS 获取临时凭证，避免明文泄露。
- 高效地连接 ClickHouse 数据库，支持连接池与预编译 SQL 提升性能。
- 封装价格数据与分类映射的读取方法，将 OSS 和 ClickHouse 的查询统一起来。

### 3. 指数计算 (calculator.py)

- **CPICalculator** 核心流程：
  ```
  compute()
  ├─ _validate_input()       # 数据校验
  ├─ _get_leaf_categories()  # 识别末级分类
  ├─ _prepare_price_comparison()  # 准备价格对比
  ├─ _merge_product_info()   # 合并商品信息
  ├─ _calculate_category_index()  # 分类指数计算
  └─ _calculate_weighted_cpi()    # 加权计算总指数
  ```

根据论文中描述的方法，总结计算消费者价格指数（CPI）的关键步骤如下：

- **CPI 计算方法总结**
- 类别内价格变化计算
  • 几何平均法：计算每个类别内所有商品的日价格变化率，使用未加权几何平均：

  $$
  {t, t-1}^{j} = \prod_{i}\left(\frac{p_{t}^{i}}{p_{t-1}^{i}}\right)^{1/ n_{j,t}}
  其中，\(p_t^i\) 为商品i在时间t的价格，\(n_{j,t}\) 为类别j中当日有效商品数量。
  $$
- 类别指数构建
  • 累积价格变化：将每日价格变化率连续相乘，生成类别指数：

  $$
  t{p}_t^j = R_{1,0}^j \cdot R_{2,1}^j \ldots R_{t,t-1}^j
  $$
- 总指数加权汇总
  • 加权算术平均：将各分类指数按官方权重汇总为总CPI：

  $$
  t} = \sum_{j}\frac{w^{j}}{W}\dot{p}_{t}^{j}$$  
  其中，\(w^j\) 为类别j的官方权重，\(W\) 为总权重。
  $$

### 4. 可视化输出 (visualizer.py)

- 基于Matplotlib/Plotly生成交互式图表

## 数据流分析

```
OSS存储
├─ products_price.csv  # 价格数据
└─ meta/category_mapping.csv  # 分类映射

ClickHouse引擎
├─ 外部表指向OSS CSV文件
└─ 提供SQL查询接口

内存数据流
Price DataFrame ─→ Category DataFrame ─→ Result DataFrame ─→ Report
[product_id,date,price,sales_volume]    [id,parent,weight]    [date,cpi_index]
```

## 配置项映射


| 模块       | 配置项              | 用途说明                     |
| ---------- | ------------------- | ---------------------------- |
| 数据加载   | OSS.ENDPOINT/BUCKET | OSS连接信息                  |
|            | DATABASE.HOST/PORT  | ClickHouse连接信息           |
| 指数计算   | ALGORITHM           | 算法类型(chain/fixed)        |
|            | ALGORITHM.base_date | 定基算法基期                 |
| 可视化输出 | OUTPUT.REPORT       | 报告输出路径                 |
|            | OUTPUT.PLOT_ENGINE  | 渲染引擎(quickbi/matplotlib) |
