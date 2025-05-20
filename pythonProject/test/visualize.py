import plotly.express as px
import pandas as pd
from pathlib import Path

def plot_cpi_trend(cpi_series: pd.Series):
    """绘制CPI趋势图（使用Plotly）"""
    fig = px.line(
        cpi_series,
        labels={'index': 'Date', 'value': 'CPI'},
        title='Daily Consumer Price Index Trend',
        markers=True
    )
    fig.update_traces(line=dict(color='steelblue'), marker=dict(size=6))
    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        hovermode='x unified'
    )
    fig.write_image("2.png", width=1200, height=500, scale=2)
    print("图表已保存为 2.png")
    fig.show()

if __name__ == '__main__':
    # 读取原始 CSV
    path = Path(__file__).resolve().parent / 'f_daily_cpi.csv'
    df = pd.read_csv(path)

    # 重命名列
    df = df.rename(columns={'Unnamed: 0': 'date', '0': 'cpi'})

    # 去除 CPI 为 NaN 的行
    df = df.dropna(subset=['cpi'])

    # 转换数据类型
    df['date'] = pd.to_datetime(df['date'])
    df['cpi'] = df['cpi'].astype(float)

    # 设置索引并绘图
    daily_cpi = df.set_index('date')['cpi']
    plot_cpi_trend(daily_cpi)
