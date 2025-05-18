import clickhouse_driver
import numpy as np
import pandas as pd
from datetime import date
from config import settings
from pathlib import Path
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class CPICalculator:
    def __init__(self, db_config):
        self.db_config = db_config
        self.clickhouse_client = self._connect_clickhouse()
        self.sqlalchemy_engine = self._connect_sqlalchemy()
        self.Session = sessionmaker(bind=self.sqlalchemy_engine)
        self.categories = self._load_categories()
        self.products = self._load_products()

    def _connect_clickhouse(self):
        """连接到 ClickHouse 数据库"""
        return clickhouse_driver.Client(
            host=self.db_config['HOST'],
            port=self.db_config['PORT'],
            user=self.db_config['USER'],
            password=self.db_config['PASSWORD']
        )

    def _connect_sqlalchemy(self):
        """连接 SQLAlchemy 引擎"""
        return create_engine(self.db_config['SQLALCHEMY_DATABASE_URI'])

    def _execute_clickhouse_query(self, query):
        """执行 ClickHouse 查询"""
        return self.clickhouse_client.execute(query)

    def _load_categories(self):
        """加载分类信息并标记叶子节点"""
        query = """
        SELECT 
            category_id,
            parent,
            weight,
            NOT EXISTS (
                SELECT 1 
                FROM categories c2 
                WHERE c2.parent = categories.category_id
            ) AS is_leaf
        FROM categories
        """
        result = self._execute_clickhouse_query(query)

        # 显式指定列名，避免隐式转换问题
        df = pd.DataFrame(result, columns=["category_id", "parent", "weight", "is_leaf"])

        # 调试：检查列名和数据类型
        print("DataFrame列名:", df.columns.tolist())
        print("is_列数据类型:", df['is_leaf'].dtype)
        print("前5行数据:\n", df.head())

        # 筛选叶子节点（is_leaf=1）
        self.leaf_categories = df[df['is_leaf'] == 1][['category_id', 'weight']]

        # 验证权重和是否为1（可选）
        total_weight = self.leaf_categories['weight'].sum()
        if not np.isclose(total_weight, 1.0, atol=1e-3):
            print(f"警告：叶子节点权重和为 {total_weight:.4f}")

        return df

    def _load_products(self):
        """加载产品信息"""
        query = "SELECT product_id, category_id FROM products"
        result = self._execute_clickhouse_query(query)
        return pd.DataFrame(result, columns=["product_id", "category_id"])

    def _load_prices_for_dates(self, date_tuple):
        """加载指定日期的价格数据"""
        date_list = "', '".join(str(d) for d in date_tuple)
        query = f"""
            SELECT product_id, price, toDate(date) AS date
            FROM prices
            WHERE date IN ('{date_list}')
        """
        result = self._execute_clickhouse_query(query)
        return pd.DataFrame(result, columns=["product_id", "price", "date"])

    def compute_daily_cpi(self, start_date: date, end_date: date) -> pd.Series:
        """计算每日CPI指数"""
        # 加载价格数据并预处理
        all_dates = pd.date_range(start_date, end_date, freq='D').date
        price_data = self._load_prices_for_dates(all_dates)
        price_pivot = price_data.pivot_table(
            index='product_id',
            columns='date',
            values='price',
            aggfunc='first'
        ).ffill(axis=1)

        # 合并产品、分类和基期价格
        merged_data = self.products.merge(
            self.leaf_categories,
            on='category_id',
            how='inner'
        ).merge(
            price_pivot[start_date].rename('base_price'),
            left_on='product_id',
            right_index=True
        )

        # 逐日计算CPI
        cpi_series = pd.Series(index=all_dates, dtype='float64')
        for current_date in all_dates:
            current_prices = price_pivot[current_date].rename('current_price')
            daily_data = merged_data.merge(
                current_prices,
                left_on='product_id',
                right_index=True,
                how='inner'
            )

            valid_data = daily_data[
                (daily_data['base_price'] > 0) &
                (daily_data['current_price'].notnull())
                ].copy()

            if valid_data.empty:
                cpi_series[current_date] = np.nan
                continue

            valid_data['log_ratio'] = np.log(valid_data['current_price'] / valid_data['base_price'])
            category_index = valid_data.groupby('category_id')['log_ratio'].mean().apply(np.exp)

            final_data = category_index.reset_index(name='price_index').merge(
                self.leaf_categories,
                on='category_id'
            )
            cpi_series[current_date] = (final_data['price_index'] * final_data['weight']).sum().round(4)

        return cpi_series


def plot_cpi_trend(cpi_series: pd.Series):
    """绘制CPI趋势图"""
    plt.figure(figsize=(15, 6))
    cpi_series.plot(
        kind='line',
        title='Daily Consumer Price Index Trend',
        xlabel='Date',
        ylabel='CPI',
        grid=True,
        color='steelblue',
        marker='o',
        markersize=4
    )
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig('1.png', dpi=300)
    print(f"图表已保存")
    plt.show()


if __name__ == '__main__':
    settings.from_env('prod')
    calculator = CPICalculator(db_config=settings.CLICKHOUSE)

    start_date = date(2025, 5, 17)
    end_date = date(2028, 5, 15)

    daily_cpi = calculator.compute_daily_cpi(start_date, end_date)
    print(daily_cpi)
    plot_cpi_trend(daily_cpi)