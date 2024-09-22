import os
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import pytz
from datetime import datetime, timedelta
import holidays
import time
import schedule

import FinanceDataReader as fdr
# import yfinance as yf

import requests
from requests.exceptions import RequestException
from retrying import retry

import pandas as pd

class MarketDataCollector:
    def __init__(self, config_path):
        self.load_config(config_path)
        self.setup_logging()
        self.setup_timezone()
        self.setup_database()


    # 설정파일 불러오기
    def load_config(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)


    # 로그 생성
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)


    # timezone 설정
    def setup_timezone(self):
        self.eastern = pytz.timezone('America/New_York')


    # db 주소 불러오기
    def get_database_url(self):
        db_config = self.config['database']
        password = os.environ.get('DB_PASSWORD', db_config.get('password', ''))

        return f"postgresql://{db_config['user']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['name']}"


    # DB 기본 세팅
    def setup_database(self):
        database_url = self.get_database_url()
        self.engine = create_engine(database_url)
        self.metadata = MetaData()

        # S&P 500 리스트를 위한 테이블 정의
        self.snp500_table = Table('snp500_list', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('symbol', String, unique=True),
            Column('name', String),
            Column('sector', String),
            Column('industry', String)
        )

        # 주식 데이터를 위한 테이블 정의
        self.stock_data_table = Table('stock_data', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('date', Date),
            Column('symbol', String),
            Column('name', String),
            Column('sector', String),
            Column('industry', String),
            Column('open', Float),
            Column('high', Float),
            Column('low', Float),
            Column('close', Float),
            Column('volume', Integer),
            Column('adj_close', Float),
            Column('change', Float),
            Column('adj_change', Float),
            Column('adj_amount', Float)
        )

        # DB 생성
        self.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)


    # 장 상태 확인
    def check_market_status(self):
        now = datetime.now(self.eastern)
        us_holidays = holidays.US()

        if now.date() in us_holidays:
            return "CLOSED_HOLIDAY"
        elif now.weekday() >= 5:
            return "CLOSED_WEEKEND"
        elif now.hour < 9:
            return "BEFORE_OPEN"
        elif now.hour >= 16:
            return "AFTER_OPEN"
        return "OPEN"


    # S&P 500 리스트 업데이트
    def update_snp500_list(self):
        try:
            df_SnP_list = fdr.StockListing('S&P500')
        except Exception:
            self.logger.warning("Failed to fetch S&P 500 list from FinanceDataReader. Using Wikipedia.")
            url = 'https://en.wikipedia.org/wiki/List_of_S&P_500_companies'
            df_SnP_list = pd.read_html(url, header=0)[0]
            cols_ren = {'Security':'name', 'Ticker symbol':'symbol', 'GICS Sector':'sector', 'GICS Sub-Industry':'industry'}
            df_SnP_list = df_SnP_list.rename(columns=cols_ren)
            df_SnP_list = df_SnP_list[['symbol', 'name', 'sector', 'industry']]
            df_SnP_list['symbol'] = df_SnP_list['symbol'].str.replace('.', '', regex=False)

        if len(df_SnP_list) != 503:
            self.logger.error(f"Unexpected number of companies in S&P 500 list: {len(df_SnP_list)}")
            return
        
        df_SnP_list.loc[df_SnP_list['Symbol'] == 'BRKB', 'Symbol'] = 'BRK-B'
        df_SnP_list.loc[df_SnP_list['Symbol'] == 'BFB', 'Symbol'] = 'BF-B'

        session = self.Session()
        try:
            session.execute(self.snp500_table.delete())
            for _, row in df_SnP_list.iterrows():
                session.execute(self.snp500_table.insert().values(
                    symbol=row['symbol'],
                    name=row['name'],
                    sector=row['sector'],
                    industry=row['industry']
                ))
            session.commit()
            self.logger.info("S&P 500 list updated successfully")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error updating S&P 500 list: {e}")
        finally:
            session.close()


    # S&P 500 리스트 불러오기
    def get_snp500_symbols(self):
        session = self.Session()
        try:
            result = session.execute(self.snp500_table.select())
            return [row['symbol'] for row in result]
        except SQLAlchemyError as e:
            self.logger.error(f"Error fetching S&P 500 symbols: {e}")
            return []
        finally:
            session.close()

    
    # S&P 500 Data Daily Update
    def get_stock_data(self, symbol):
        try:
            # start_time = datetime.now(self.eastern) - timedelta(days=0)
            # end_time = datetime.now(self.eastern)
            # tp = yf.download(symbol, start=start_time, end=end_time)
            start_time = pd.to_datetime((datetime.today() - timedelta(days = 0)).strftime('%Y-%m-%d'))
            end_time = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
            start_ts = int(time.mktime(start_time.timetuple()))
            end_ts = int(time.mktime(end_time.timetuple()))
            
            url = (
                f'https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?'
                f'period1={start_ts}&period2={end_ts}&interval=1d&includeAdjustedClose=true'
            )
            response = requests.get(url, headers={'user-agent': 'Mozilla/5.0 AppleWebKit/537.36'})
            response.raise_for_status()
            jo = response.json()

            index = pd.to_datetime(jo['chart']['result'][0]['timestamp'], unit='s').normalize()
            values  = {**jo['chart']['result'][0]['indicators']['quote'][0], **jo['chart']['result'][0]['indicators']['adjclose'][0]}

            col_map = {'index':'date', 'adjclose':'adj_close'}
            tp = pd.DataFrame(data=values, index=index)
            tp = tp.reset_index().rename(columns=col_map)
            tp = tp[col_map.values()].reset_index()
            tp['date'] = tp['date'].astype(str)

            return tp
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return None


    # S&P 500 Data Daily Save
    def save_stock_data(self, symbol, data):
        if data.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return

        session = self.Session()

        # symbol에 해당하는 변수 추출 조건
        stmt = select([self.snp500_table.c.name, self.snp500_table.c.sector, self.snp500_table.c.industry]).where(self.snp500_table.c.symbol == symbol)
        # symbol에 해당하는 변수 추출
        result = session.execute(stmt)
        row_symbol = result.fetchone()

        try:
            for index, row in data.iterrows():
                session.execute(self.stock_data_table.insert().values(
                    date=index.date(),
                    symbol=symbol,
                    name=row_symbol['name'],
                    sector=row_symbol['sector'],
                    industry=row_symbol['industry'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume'],
                    adj_close=row['adj_close'],
                    change=(row['close'] - row['open']) / row['open'],
                    adj_change=(row['adj_close'] - row['open']) / row['open'],
                    adj_amount=row['adj_close'] * row['volume']
                ))
            session.commit()
            self.logger.info(f"Stock data saved for {symbol}")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error saving stock data for {symbol}: {e}")
        finally:
            session.close()


    # S&P 500 Data Daily Load
    def collect_stock_prices(self):
        self.logger.info("Collecting stock prices")
        symbols = self.get_snp500_symbols()

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {executor.submit(self.get_stock_data, symbol): symbol for symbol in symbols}
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    data = future.result()
                    if data is not None and not data.empty:
                        self.save_stock_data(symbol, data)
                except Exception as e:
                    self.logger.error(f"Error processing {symbol}: {e}")

    
    def collect_news(self):
        self.logger.info("Collecting news")
        # 뉴스 수집 로직 구현

    def analyze_stock(self):
        self.logger.info("Analyzing stock")
        # 주식 분석 로직 구현


    # Make.com으로 트리거 보내기
    def send_signal_to_make(self):
        try:
            webhook_url = self.config['webhook_url_his']
            data = {"stock_list": ['dummy']}  # 실제 데이터로 교체 필요
            response = requests.post(webhook_url, json=data)
            response.raise_for_status()
            self.logger.info("Signal sent to Make.com successfully")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending signal to Make.com: {e}")
    

    # 잡 설정
    def job(self):
        status = self.check_market_status()
        self.logger.info(f"Market status: {status}")

        if status == "BEFORE_OPEN":
            self.update_snp500_list()
            # self.collect_news()
            # self.send_signal_to_make()
        elif status == "AFTER_OPEN":
            # self.collect_news()
            self.collect_stock_prices()
            # self.analyze_stock()
            # self.send_signal_to_make()


    # 스케줄링
    def schedule_jobs(self):
        now = datetime.now(self.eastern)
        # 서머타임이면, 정규장 한국 시간 오후 10시 30분 ~ 오전 5시 / 프리마켓 오후 5시 / 애프터마켓 오전 8시
        if now.dst():
            before_time = "22:00"
            after_time = "05:30"
        # 서머타임 아니면, 정규장 한국 시간 오후 11시 30분 ~ 오전 6시 / 프리마켓 오후 6시 / 애프터마켓 오전 9시
        else:
            before_time = "23:00"
            after_time = "06:30"

        schedule.every().day.at(before_time).do(self.job)
        schedule.every().day.at(after_time).do(self.job)

        self.logger.info(f"Jobs scheduled at {before_time} and {after_time}")


    # 
    def run(self):
        self.logger.info("Starting market data collector")
        self.schedule_jobs()

        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"An error occurred in the main loop: {e}")
                time.sleep(60)  # 1분 대기 후 재시도


if __name__ == "__main__":
    collector = MarketDataCollector('config.yaml')
    collector.run()