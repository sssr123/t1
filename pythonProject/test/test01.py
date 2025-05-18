from clickhouse_driver import Client
from config import settings
# 配置连接参数
client = Client(
    host= settings.CLICKHOUSE.HOST,      # ClickHouse服务器地址
    port=3306,              # 默认TCP端口9000（HTTP API端口通常是8123）
    user= settings.DATABASE.USER,         # 用户名（默认是'default'）
    password= settings.DATABASE.PASSWORD,            # 密码（如果设置了）
    database= 'default',     # 默认数据库
    settings={'use_numpy': True}  # 可选设置
)

try:
    # 测试连接
    result = client.execute('SELECT * from categories')
    print("Connection successful. Result:", result)
except Exception as e:
    print("Connection failed:", e)