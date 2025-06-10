import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
from tqdm import tqdm
from polygon import RESTClient
import os
from dotenv import load_dotenv


class StockDataFetcher:
    """
    股票数据获取类，负责从AKShare获取股票的历史K线数据、财务数据和新闻信息
    """
    
    def __init__(self):
        # 加载环境变量
        load_dotenv(override=True)
        api_key = os.getenv('POLYGON_API_KEY')
        self.client = RESTClient(api_key)
    
    def fetch_stock_data(self, stock_code, period='1年'):
        """
        获取股票的历史K线数据
        
        参数:
            stock_code (str): 股票代码，如 '000001'
            period (str): 获取数据的时间周期，默认为'1年'
            
        返回:
            pandas.DataFrame: 包含股票历史数据的DataFrame
        """
        # 确保股票代码格式正确
        if not stock_code.isdigit():
            # 根据股票代码判断交易所
            # if stock_code.startswith(('0', '3')):
            #     stock_code = f"sz{stock_code}"
            # elif stock_code.startswith(('6', '9')):
            #     stock_code = f"sh{stock_code}"
            #取消前缀sz或者sh
            stock_code = stock_code.lstrip('sz').lstrip('sh').rstrip('SZ').rstrip('SH')
            # 取消后缀.sz或者.sh
            stock_code = stock_code.rstrip('.sz').rstrip('.sh').rstrip('.SZ').rstrip('.SH')
        
        # 计算开始日期
        if period == '1年':
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        elif period == '6个月':
            start_date = (datetime.now() - timedelta(days=183)).strftime('%Y%m%d')
        elif period == '3个月':
            start_date = (datetime.now() - timedelta(days=91)).strftime('%Y%m%d')
        elif period == '1个月':
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        elif period == '1周':
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        else:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        # 计算结束日期
        end_date = datetime.now().strftime('%Y%m%d')
        try:
            # 使用akshare获取股票历史数据
            stock_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                                           start_date=start_date, end_date=end_date, 
                                           adjust="qfq")
            
            # 重命名列以便后续处理
            stock_data.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover'
            }, inplace=True)
            
            # 将日期列转换为日期时间格式
            stock_data['date'] = pd.to_datetime(stock_data['date'])
            
            return stock_data
            
        except Exception as e:
            print(f"获取股票数据时出错: {e}")
            return pd.DataFrame()
    
    def fetch_financial_data(self, stock_code):
        """
        获取股票的财务数据
        
        参数:
            stock_code (str): 股票代码，如 '000001'
            
        返回:
            dict: 包含财务数据的字典
        """
        financial_data = {}
        
        try:
            # 获取股票基本信息
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            if not stock_info.empty:
                financial_data['基本信息'] = stock_info.set_index('item').to_dict()['value']
            
            # 获取关键指标
            financial_abstract = ak.stock_financial_abstract(symbol=stock_code)
            if not financial_abstract.empty:
                # 只取最新的财务指标
                latest_indicator = financial_abstract.iloc[:, 1:3].dropna().set_index('指标').to_dict()
                financial_data['关键指标'] = latest_indicator

            return financial_data
            
        except Exception as e:
            print(f"获取财务数据时出错: {e}")
            return financial_data
    
    def fetch_news_data(self, stock_code, max_items=10):
        """
        获取与股票相关的新闻信息
        
        参数:
            stock_code (str): 股票代码，如 '000001'
            max_items (int): 最大获取新闻条数
            
        返回:
            list: 包含新闻数据的列表
        """
        news_list = []
        
        try:
            # 获取股票名称
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            if not stock_info.empty:
                stock_name = stock_info.loc[stock_info['item'] == '股票简称', 'value'].values[0]
                
                # 获取股票相关新闻
                news_data = ak.stock_news_em(symbol=stock_code)
                
                if not news_data.empty:
                    # 限制新闻条数
                    news_data = news_data.head(max_items)
                    
                    for _, row in news_data.iterrows():
                        news_item = {
                            'title': row['新闻标题'],
                            'date': row['发布时间'],
                            'content': row['新闻内容'] if '新闻内容' in row else '',
                        }
                        news_list.append(news_item)
                
            return news_list
            
        except Exception as e:
            print(f"获取新闻数据时出错: {e}")
            return news_list
    
    def fetch_us_stock_data(self, stock_code, period='1年'):
        """
        获取美股的历史K线数据
        
        参数:
            stock_code (str): 股票代码，如 'AAPL'
            period (str): 获取数据的时间周期，默认为'1年'
            
        返回:
            pandas.DataFrame: 包含股票历史数据的DataFrame
        """
        
        # 计算开始日期
        # Polygon要求时间格式为：YYYY-MM-DD 或 millisecond timestamp
        if period == '1年':
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        elif period == '6个月':
            start_date = (datetime.now() - timedelta(days=183)).strftime('%Y-%m-%d')
        elif period == '3个月':
            start_date = (datetime.now() - timedelta(days=91)).strftime('%Y-%m-%d')
        elif period == '1个月':
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        else:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # 计算结束日期
        end_date = datetime.now().strftime('%Y-%m-%d')

        try:
            # 获取股票历史数据 aggs数据类型是generator
            aggs = self.client.list_aggs(
                stock_code,
                1,
                "day",
                start_date,
                end_date,
                adjusted=True
            )
            
            # 转换为DataFrame
            if not aggs:
                print(f"Polygon.io未能获取到 {stock_code} 的数据")
                return None
            
            # 转换为DataFrame
            data = pd.DataFrame([{
                'date': 'date',
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume,
                'date': datetime.fromtimestamp(agg.timestamp/1000)
            } for agg in aggs])
            
            return data
                
        except Exception as e:
            print(f"获取美股数据时出错: {e}")
            return pd.DataFrame()
    
    def fetch_us_financial_data(self, stock_code):
        """
        获取美股的财务数据
        
        参数:
            stock_code (str): 股票代码，如 'AAPL'
            
        返回:
            dict: 包含财务数据的字典
        """
        pass

    def fetch_us_news_data(self, stock_code, max_limit=5):
        """
        获取与美股相关的新闻信息
        
        参数:
            stock_code (str): 股票代码，如 'AAPL'
            max_items (int): 最大获取新闻条数
            
        返回:
            list: 包含新闻数据的列表
        """
        
        news_list = []
        news_articles = self.client.list_ticker_news(
            stock_code, 
            params={"published_utc.gte": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')}, # 获取过去24小时内的新闻
            order="desc", 
            limit=max_limit
            )

        try:
            # Display the title and insights for each article
            for _, article in enumerate(news_articles):
                if _ == 5:
                    break
                for _, insight in enumerate(article.insights):
                    if insight.ticker == stock_code:
                        news_item = {
                            'title': article.title,
                            'date': article.published_utc,
                            'content': article.description,
                            'sentiment': insight.sentiment,
                            'sentiment_reasoning': insight.sentiment_reasoning,
                        }
                        news_list.append(news_item)
            return news_list
            
        except Exception as e:
            print(f"获取美股新闻时出错: {e}")
            return news_list

