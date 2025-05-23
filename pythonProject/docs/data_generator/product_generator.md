# 商品生成器

## 核心逻辑

该商品生成器用于模拟生成天猫商品清单，支持后续的搜索结果模拟。它根据提供的输入参数生成指定数量的商品，并以JSON格式返回商品列表。

## 输入值

- `category_name`: 商品类别名称
- `count`: 商品数量，默认为500个

## 输出值

返回一个商品列表，每个商品包含以下字段：
- `product_name`: 商品名称
- `price`: 商品价格

## 生成逻辑

### 商品名称生成

商品名称基于`category`生成，格式为`[category] Product [index]`，其中`index`为商品的序号（从1开始）。
   
### 商品价格生成

商品价格随机生成，范围在50到500之间，保留两位小数。
