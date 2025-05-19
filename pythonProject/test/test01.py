from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, SocketTimeoutError, ServerException
from config import settings
# 配置连接参数

try:
    client = Client(
        host= settings.CLICKHOUSE.HOST,      # ClickHouse服务器地址
        port=3306,              # 默认TCP端口9000（HTTP API端口通常是8123）
        user= settings.DATABASE.USER,         # 用户名（默认是'default'）
        password= settings.DATABASE.PASSWORD,            # 密码（如果设置了）
        database= 'default',     # 默认数据库
        settings={'use_numpy': True}  # 可选设置
    )

    result = client.execute('SELECT * from categories')
    print("Connection successful. Result:", result)

except ServerException as e:
    if "Code: 516" in str(e):  # 认证失败
        print("❌ Authentication failed. Please check your username and password.")
    elif "Code: 81" in str(e):  # 数据库不存在
        print(f"❌ Database does not exist. Error: {e}")
    else:
        print(f"❌ Query execution failed (ClickHouse Server Error): {e}")

except SocketTimeoutError:
    print("❌ Connection timed out. Possible reasons:")
    print("- ClickHouse server is not reachable")
    print("- Network issues or firewall blocking")
    print(f"- Host: {settings.CLICKHOUSE.HOST}, Port: 9000")

except NetworkError as e:
    print("❌ Network error. Possible reasons:")
    print("- ClickHouse server is not running")
    print("- Incorrect host or port")
    print("- Connection refused (check if ClickHouse is listening)")
    print(f"Error details: {e}")
