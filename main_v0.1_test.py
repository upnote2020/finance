import os
import logging
from logging.handlers import RotatingFileHandler
import yaml
from typing import List, Optional, Tuple

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

import pytz
from datetime import datetime, timedelta
import holidays
import time
import schedule

import aiohttp
import asyncio
from retrying import retry

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import pandas as pd

class SlackHandler(logging.Handler):
    def __init__(self, slack_token: str, channel: str):
        super().__init__()
        self.client = WebClient(token=slack_token)
        self.channel = channel

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            self.client.chat_postMessage(channel=self.channel, text=message)
        except SlackApiError as e:
            print(f"Error sending message to Slack: {e}")

class Config:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # 환경 변수에서 민감한 정보 로드
        self.config['database']['password'] = os.environ.get('DB_PASSWORD', self.config['database'].get('password', ''))
        self.config['slack_token'] = os.environ.get('SLACK_TOKEN', self.config.get('slack_token', ''))
        self.test_date = self.config.get('test_date')

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.config.get(key, default)

class MarketDataCollector:
    def __init__(self, config_path: str):
        self.config = Config(config_path)
        self.setup_logging()
        self.setup_timezone()
        self.setup_database_connection()

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

    def setup_timezone(self) -> None:
        self.eastern = pytz.timezone('America/New_York')

    def get_database_url(self) -> str:
        db_config = self.config.config['database']
        return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"

    def setup_database_connection(self) -> None:
        database_url = self.get_database_url()
        self.engine = create_engine(database_url, poolclass=QueuePool, pool_size=5, max_overflow=10)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        self.create_tables()

    def create_tables(self) -> None:
        self.snp500_table = Table('snp500_list', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('symbol', String, unique=True),
            Column('name', String),
            Column('sector', String),
            Column('industry', String)
        )

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
            Column('volume', Float),
            Column('adj_close', Float),
            Column('change', Float),
            Column('adj_change', Float),
            Column('adj_amount', Float)
        )

        self.metadata.create_all(self.engine)
        self.logger.info("Database tables created or verified.")

    def get_current_date(self) -> datetime:
        if self.config.test_date:
            return datetime.strptime(self.config.test_date, '%Y-%m-%d').replace(tzinfo=self.eastern)
        return datetime.now(self.eastern)

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

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def fetch_snp500_list(self) -> pd.DataFrame:
        try:
            return pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', header=0)[0]
        except Exception as e:
            self.logger.error(f"Failed to fetch S&P 500 list: {e}")
            raise

    def update_snp500_list(self) -> None:
        df = self.fetch_snp500_list()

        df.rename(columns={
            'Symbol': 'symbol',
            'Security': 'name',
            'GICS Sector': 'sector',
            'GICS Sub-Industry': 'industry'
        }, inplace=True)

        df['symbol'] = df['symbol'].str.replace('.', '-', regex=False)

        session = self.Session()
        try:
            session.execute(self.snp500_table.delete())
            records = df[['symbol', 'name', 'sector', 'industry']].to_dict(orient='records')
            session.bulk_insert_mappings(self.snp500_table, records)
            session.commit()
            self.logger.info("S&P 500 list updated successfully")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error updating S&P 500 list: {e}")
        finally:
            session.close()

    def get_snp500_symbols(self) -> List[str]:
        session = self.Session()
        try:
            result = session.execute(select([self.snp500_table.c.symbol]))
            return [row['symbol'] for row in result]
        except SQLAlchemyError as e:
            self.logger.error(f"Error fetching S&P 500 symbols: {e}")
            return []
        finally:
            session.close()

    def format_symbol_for_yahoo(self, symbol: str) -> str:
        return symbol.replace('-', '.')

    async def fetch_stock_data_async(self, symbol: str) -> Tuple[str, Optional[pd.DataFrame]]:
        yahoo_symbol = self.format_symbol_for_yahoo(symbol)
        try:
            now = self.get_current_date()
            start_time = now - timedelta(days=5)
            end_time = now
            start_ts = int(start_time.timestamp())
            end_ts = int(end_time.timestamp())

            url = (
                f'https://query2.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?'
                f'period1={start_ts}&period2={end_ts}&interval=1d&includeAdjustedClose=true'
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={'user-agent': 'Mozilla/5.0'}) as response:
                    response.raise_for_status()
                    jo = await response.json()

            timestamps = jo['chart']['result'][0]['timestamp']
            indicators = jo['chart']['result'][0]['indicators']['quote'][0]
            adj_close = jo['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']

            data = pd.DataFrame(indicators)
            data['adj_close'] = adj_close
            data['date'] = pd.to_datetime(timestamps, unit='s')
            data['symbol'] = symbol

            return symbol, data
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return symbol, None

    def save_stock_data(self, symbol: str, data: pd.DataFrame) -> None:
        if data.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return

        session = self.Session()

        stmt = select([self.snp500_table.c.name, self.snp500_table.c.sector, self.snp500_table.c.industry]).where(self.snp500_table.c.symbol == symbol)
        result = session.execute(stmt)
        row_symbol = result.fetchone()

        try:
            data['name'] = row_symbol['name']
            data['sector'] = row_symbol['sector']
            data['industry'] = row_symbol['industry']

            records = data.to_dict(orient='records')
            session.bulk_insert_mappings(self.stock_data_table, records)
            session.commit()
            self.logger.info(f"Stock data saved for {symbol}")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error saving stock data for {symbol}: {e}")
        finally:
            session.close()

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
        # 뉴스 수집 로직 구현

    def analyze_stock(self):
        self.logger.info("Analyzing stock")
        # 주식 분석 로직 구현

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def send_signal_to_make(self) -> None:
        try:
            webhook_url = self.config.config['webhook_url_his']
            data = {"stock_list": ['dummy']}  # TODO: 실제 데이터로 교체 필요
            response = requests.post(webhook_url, json=data, timeout=10)
            response.raise_for_status()
            self.logger.info("Signal sent to Make.com successfully")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending signal to Make.com: {e}")
            raise

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
                # self.analyze_stock()
                # self.send_signal_to_make()
        except Exception as e:
            self.logger.error(f"An error occurred during job execution: {e}")

    def schedule_jobs(self) -> None:
        now = self.get_current_date()
        # 서머타임 적용 여부 확인
        is_dst = bool(self.eastern.localize(datetime.now()).dst())

        if is_dst:
            before_time = "22:00"
            after_time = "05:30"
        else:
            before_time = "23:00"
            after_time = "06:30"

        schedule.every().day.at(before_time).do(lambda: asyncio.run(self.job()))
        schedule.every().day.at(after_time).do(lambda: asyncio.run(self.job()))

        self.logger.info(f"Jobs scheduled at {before_time} and {after_time}")

    def run(self) -> None:
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
