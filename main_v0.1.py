import os
import logging
from logging.handlers import RotatingFileHandler
import yaml
from typing import List, Any, Optional, Tuple

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

import pytz
from datetime import datetime, timedelta
import holidays
import time
import schedule

import FinanceDataReader as fdr
import yfinance as yf

import requests
import asyncio
import aiohttp
from retrying import retry

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import pandas as pd

class SlackHandler(logging.Handler):
    def __init__(self, slack_token : str, channel : str):
        super().__init__()
        self.client = WebClient(token=slack_token)
        self.channel = channel

    def emit(self, record : logging.LogRecord) -> None:
        try:
            message = self.format(record)
            self.client.chat_postMessage(channel=self.channel, text=message)
        except SlackApiError as e:
            print(f"Error sending message to Slack: {e}")



class Config:
    def __init__(self, config_path : str):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # 환경 변수에서 민감한 정보 로드
        self.config['database']['password'] = os.environ.get('DB_PASSWORD', self.config['database'].get('password', ''))
        self.config['slack_token'] = os.environ.get('SLACK_TOKEN', self.config.get('slack_token', ''))
        self.test_date = self.config.get('test_date')

    def get(self, key : str, default : Any = None) -> Any:
        return self.config.get(key, default)



class MarketDataCollector:
    def __init__(self, config_path : str):
        self.config = Config(config_path)
        self.setup_logging()
        self.setup_timezone()
        self.setup_database_connection()


    # 로그 생성
    def setup_logging(self) -> None:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # 로컬에 5개까지 파일 누적 저장
        file_handler = RotatingFileHandler('market_data_collector.log', maxBytes=10485760, backupCount=5)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)

        slack_handler = SlackHandler(self.config.get('slack_token'), self.config.get('slack_channel'))
        slack_handler.setLevel(logging.WARNING)  # WARNING 이상 레벨의 로그만 Slack으로 전송
        self.logger.addHandler(slack_handler)


    # timezone 설정
    def setup_timezone(self) -> None:
        self.eastern = pytz.timezone('America/New_York')


    # db 주소 불러오기
    def get_database_url(self) -> str:
        db_config = self.config['database']
        return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"


    # db 연결
    def setup_database_connection(self) -> None:
        database_url = self.get_database_url()
        self.engine = create_engine(database_url, poolclass=QueuePool, pool_size=5, max_overflow=10)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)


    # DB 기본 세팅
    def create_tables(self) -> None:
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
        self.logger.info("Database tables created or verified.")


    # 날짜 취득
    def get_current_date(self) -> datetime:
        if self.config.test_date:
            return datetime.strptime(self.config.test_date, '%Y-%m-%d').replace(tzinfo=self.eastern)
        return datetime.now(self.eastern)


    # 장 상태 확인
    def check_market_status(self) -> str:
        now = self.get_current_date()
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


    # 에러 처리 및 재시도 로직
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def fetch_snp500_list(self) -> pd.DataFrame:
        try:
            df = fdr.StockListing('S&P500')
            cols_ren = {'Symbol':'symbol', 'Name':'name', 'Sector':'sector', 'Industry':'industry'}
            df = df.rename(cols_ren)
            return df
        except Exception as e:
            self.logger.warning(f"Failed to fetch S&P 500 list from FinanceDataReader: {e}")
            url = 'https://en.wikipedia.org/wiki/List_of_S&P_500_companies'
            df = pd.read_html(url, header=0)[0]
            cols_ren = {'Security':'name', 'Ticker symbol':'symbol', 'GICS Sector':'sector', 'GICS Sub-Industry':'industry'}
            df = df.rename(columns=cols_ren)
            return df


    # S&P 500 리스트 업데이트
    def update_snp500_list(self) -> None:
        df_SnP_list = self.fetch_snp500_list()
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
            # 1안
            # records = df_SnP_list[['symbol', 'name', 'sector', 'industry']].to_dict(orient='records')
            # session.bulk_insert_mappings(self.snp500_table, records)
            # 2안
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
    def get_snp500_symbols(self) -> List[str]:
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
    async def fetch_stock_data_async(self, symbol : str) -> Tuple[str, Optional[pd.DataFrame]]:
        try:
            # start_time = datetime.now(self.eastern) - timedelta(days=0)
            # end_time = datetime.now(self.eastern)
            # tp = yf.download(symbol, start=start_time, end=end_time)
            now = self.get_current_date()
            start_time = pd.to_datetime((now - timedelta(days = 0)).strftime('%Y-%m-%d'))
            end_time = pd.to_datetime(now.strftime('%Y-%m-%d'))
            start_ts = int(time.mktime(start_time.timetuple()))
            end_ts = int(time.mktime(end_time.timetuple()))
            
            url = (
                f'https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?'
                f'period1={start_ts}&period2={end_ts}&interval=1d&includeAdjustedClose=true'
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={'user-agent': 'Mozilla/5.0 AppleWebKit/537.36'}) as response:
                    response.raise_for_status()
                    jo = await response.json()

            index = pd.to_datetime(jo['chart']['result'][0]['timestamp'], unit='s').normalize()
            values = {**jo['chart']['result'][0]['indicators']['quote'][0], **jo['chart']['result'][0]['indicators']['adjclose'][0]}

            col_map = {'index':'date', 'open':'open', 'high':'high', 'low':'low', 'close':'close', 'volume':'volume', 'adjclose':'adj_close'}
            tp = pd.DataFrame(data=values, index=index)
            tp = tp.reset_index().rename(columns=col_map)
            tp = tp[col_map.values()]
            tp['date'] = tp['date'].astype(str)

            return symbol, tp
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return symbol, None


    # S&P 500 Data Daily Save
    def save_stock_data(self, symbol : str, data : pd.DataFrame) -> None:
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
            # 1안
            # data['name'] = row_symbol['name']
            # data['sector'] = row_symbol['sector']
            # data['industry'] = row_symbol['industry']
            # change=(row['close'] - row['open']) / row['open'] # 반영되게 수정 필요
            # adj_change=(row['adj_close'] - row['open']) / row['open'] # 반영되게 수정 필요
            # adj_amount=row['adj_close'] * row['volume'] # 반영되게 수정 필요
            # records = data.to_dict(orient='records')
            # session.bulk_insert_mappings(self.stock_data_table, records)
            # 2안
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
    async def collect_stock_prices_async(self) -> None:
        self.logger.info("Collecting stock prices")
        symbols = self.get_snp500_symbols()
        semaphore = asyncio.Semaphore(10)

        async def fetch_with_semaphore(symbol):
            async with semaphore:
                return await self.fetch_stock_data_async(symbol)

        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)

        for symbol, data in results:
            if data is not None and not data.empty:
                self.save_stock_data(symbol, data)


    def collect_news(self):
        self.logger.info("Collecting news")
        symbol = 'AAPL'
        ticker = yf.Ticker(symbol)
        news = ticker.news
        # 뉴스 수집 로직 구현


    def analyze_stock(self) -> List[str]:
        self.logger.info("Analyzing stock")
        # 주식 분석 로직 구현


    # Make.com으로 트리거 보내기
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def send_signal_to_make(self, stock_list : List[str]) -> None:
        try:
            webhook_url = self.config['webhook_url_his']
            data = {"stock_list": ['dummy']}  # TODO: 실제 데이터로 교체 필요, stock_list
            response = requests.post(webhook_url, json=data, timeout=10)
            response.raise_for_status()
            self.logger.info("Signal sent to Make.com successfully")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending signal to Make.com: {e}")
            raise
    

    # 잡 설정
    async def job(self) -> None:
        try:
            status = self.check_market_status()
            self.logger.info(f"Market status: {status}")

            if status == "BEFORE_OPEN":
                self.update_snp500_list()
                # self.collect_news()
                # self.send_signal_to_make()
            elif status == "AFTER_OPEN":
                # self.collect_news()
                await self.collect_stock_prices_async()
                # stock_list = self.analyze_stock()
                # self.send_signal_to_make(stock_list)
        except Exception as e:
            self.logger.error(f"An error occurred during job execution: {e}")

    # 스케줄링
    def schedule_jobs(self) -> None:
        now = self.get_current_date()
        # 서머타임이면, 정규장 한국 시간 오후 10시 30분 ~ 오전 5시 / 프리마켓 오후 5시 / 애프터마켓 오전 8시
        if now.dst():
            before_time = "22:00"
            after_time = "05:30"
        # 서머타임 아니면, 정규장 한국 시간 오후 11시 30분 ~ 오전 6시 / 프리마켓 오후 6시 / 애프터마켓 오전 9시
        else:
            before_time = "23:00"
            after_time = "06:30"

        schedule.every().day.at(before_time).do(lambda: asyncio.run(self.job()))
        schedule.every().day.at(after_time).do(lambda: asyncio.run(self.job()))

        self.logger.info(f"Jobs scheduled at {before_time} and {after_time}")


    # 작동 진입점
    def run(self) -> None:
        self.create_tables() # 불필요시 비활성화
        self.logger.info("Starting market data collector")
        self.schedule_jobs()

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            self.logger.error(f"An error occurred in the main loop: {e}")
            time.sleep(60)  # 1분 대기 후 재시도
        finally:
            self.logger.info("Shutting down market data collector")



if __name__ == "__main__":
    collector = MarketDataCollector('config.yaml')
    collector.run()