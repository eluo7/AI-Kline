import os
from dotenv import load_dotenv
from modules.data_fetcher import StockDataFetcher

# 加载环境变量
load_dotenv(override=True)

def test_us_data_fetcher():
    # 检查环境变量是否加载成功
    polygon_api_key = os.getenv('POLYGON_API_KEY')
    if polygon_api_key:
        print(f"POLYGON_API_KEY 已成功加载：{polygon_api_key[:5]}...{polygon_api_key[-5:]}") # 打印部分密钥，保护隐私
    else:
        print("警告：POLYGON_API_KEY 未加载。请检查 .env 文件和 load_dotenv() 调用。")

    fetcher = StockDataFetcher()
    stock_code = "AAPL"  # 替换为您想测试的美股代码

    # print(f"\n--- 测试获取美股历史K线数据 ({stock_code}) ---")
    # us_stock_data = fetcher.fetch_us_stock_data(stock_code, period='1个月')
    # if not us_stock_data.empty:
    #     print("成功获取美股历史K线数据：")
    #     print(us_stock_data.head())
    # else:
    #     print("获取美股历史K线数据失败或数据为空。")

    print(f"\n--- 测试获取美股财务数据 ({stock_code}) ---")
    us_financial_data = fetcher.fetch_us_financial_data(stock_code)
    if us_financial_data:
        print("成功获取美股财务数据：")
        # 打印部分财务数据，避免输出过多
        print(us_financial_data.get('基本信息', {}).get('name'))
        print(us_financial_data.get('财务指标', {}).get('results', [])[0] if us_financial_data.get('财务指标', {}).get('results') else '无财务指标')
    else:
        print("获取美股财务数据失败或数据为空。")

    print(f"\n--- 测试获取美股新闻数据 ({stock_code}) ---")
    us_news_data = fetcher.fetch_us_news_data(stock_code, max_items=3)
    if us_news_data:
        print("成功获取美股新闻数据：")
        for news_item in us_news_data:
            print(f"标题: {news_item['title']}")
            print(f"日期: {news_item['date']}")
            print(f"URL: {news_item['url']}\n")
    else:
        print("获取美股新闻数据失败或数据为空。")

if __name__ == "__main__":
    test_us_data_fetcher()