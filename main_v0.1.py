import os
import logging
from logging.handlers import RotatingFileHandler
import yaml
from typing import List, Any, Optional, Tuple

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Table, MetaData, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
from sqlalchemy.dialects.postgresql import insert

import pytz
from datetime import datetime, timedelta
import holidays
import time
import schedule

import FinanceDataReader as fdr
import yfinance as yf

import requests
import psutil
# import aiohttp
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
    
    def __getitem__(self, key):
        return self.config[key]


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
        db_config = self.config.get('database', {})
        return f"postgresql://{db_config.get('user')}:{db_config.get('password')}@{db_config.get('host')}:{db_config.get('port')}/{db_config.get('name')}"


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
            Column('date', Date, primary_key=True),
            Column('symbol', String, primary_key=True),
            Column('name', String),
            Column('sector', String),
            Column('industry', String),
            Column('open', Float),
            Column('high', Float),
            Column('low', Float),
            Column('close', Float),
            Column('adj_close', Float),
            Column('volume', Integer),
            Column('change', Float),
            Column('adj_change', Float),
            Column('adj_amount', Float)
        )
        # 필요 시에만 활성화. 테이블 삭제 후 재생성
        # self.stock_data_table.drop(self.engine, checkfirst=True)
        # DB 생성
        self.metadata.create_all(self.engine)
        self.logger.info("Database tables created or verified.")


    # 날짜 취득
    def get_current_date(self) -> datetime:
        if self.config.test_date:
            return self.eastern.localize(datetime.strptime(self.config.test_date, '%Y-%m-%d %H:%M:%S'))
        return datetime.now(self.eastern)


    # 장 상태 확인
    def check_market_status(self, now : datetime) -> str:
        # now = self.get_current_date()
        us_holidays = holidays.US()
        self.logger.info(f"Current date: {now}, Weekday: {now.weekday()}, Hour: {now.hour}")
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
            df = df.rename(columns=cols_ren)
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
        
        df_SnP_list.loc[df_SnP_list['symbol'] == 'BRKB', 'symbol'] = 'BRK-B'
        df_SnP_list.loc[df_SnP_list['symbol'] == 'BFB', 'symbol'] = 'BF-B'

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
    def get_snp500_symbols(self) -> List[str]:
        session = self.Session()
        try:
            result = session.execute(self.snp500_table.select())
            return [row[1] for row in result]
        except SQLAlchemyError as e:
            self.logger.error(f"Error fetching S&P 500 symbols: {e}")
            return []
        finally:
            session.close()


    # S&P 500 Data Daily Update
    def fetch_stock_data(self, symbol : str, now : datetime) -> Tuple[str, Optional[pd.DataFrame]]:
        try:
            start_time = now - timedelta(days=0)
            end_time = now
            tp = yf.download(symbol, start=start_time, end=end_time, progress=False)
            col_map = {'Date':'date', 'Open':'open', 'High':'high', 'Low':'low', 'Close':'close', 'Adj Close':'adj_close', 'Volume':'volume'}

            # now = self.get_current_date()
            # start_time = pd.to_datetime((now - timedelta(days = 0)).strftime('%Y-%m-%d'))
            # end_time = pd.to_datetime(now.strftime('%Y-%m-%d'))
            # start_ts = int(time.mktime(start_time.timetuple()))
            # end_ts = int(time.mktime(end_time.timetuple()))
            
            # url = (
            #     f'https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?'
            #     f'period1={start_ts}&period2={end_ts}&interval=1d&includeAdjustedClose=true'
            # )

            # async with aiohttp.ClientSession() as session:
            #     async with session.get(url, headers={'user-agent': 'Mozilla/5.0 AppleWebKit/537.36'}) as response:
            #         response.raise_for_status()
            #         jo = await response.json()

            # index = pd.to_datetime(jo['chart']['result'][0]['timestamp'], unit='s').normalize()
            # values = {**jo['chart']['result'][0]['indicators']['quote'][0], **jo['chart']['result'][0]['indicators']['adjclose'][0]}

            # col_map = {'index':'date', 'open':'open', 'high':'high', 'low':'low', 'close':'close', 'adjclose':'adj_close', 'volume':'volume'}
            # tp = pd.DataFrame(data=values, index=index)

            tp = tp.reset_index().rename(columns=col_map)
            tp = tp[col_map.values()]
            tp['date'] = tp['date'].dt.strftime('%Y-%m-%d')  # Convert date to string format

            return symbol, tp
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return symbol, None


    # S&P 500 Data Daily Save
    def save_stock_data(self, index : int, symbol : str, data : pd.DataFrame, now : datetime) -> None:
        if data.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return

        session = self.Session()
        try:
            # Fetch symbol details
            stmt = select(self.snp500_table.c.name, self.snp500_table.c.sector, self.snp500_table.c.industry).where(self.snp500_table.c.symbol == symbol)
            result = session.execute(stmt)
            row_symbol = result.fetchone()

            if row_symbol is None:
                self.logger.warning(f"No matching symbol found in snp500_table for {symbol}")
                return

            for _, row in data.iterrows():
                change = (row['close'] - row['open']) / row['open'] if row['open'] != 0 else 0
                adj_change = (row['adj_close'] - row['open']) / row['open'] if row['open'] != 0 else 0
                adj_amount = row['adj_close'] * row['volume']

                insert_stmt = insert(self.stock_data_table).values(
                    date=row['date'],
                    symbol=symbol,
                    name=row_symbol.name,
                    sector=row_symbol.sector,
                    industry=row_symbol.industry,
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    adj_close=row['adj_close'],
                    volume=row['volume'],
                    change=change,
                    adj_change=adj_change,
                    adj_amount=adj_amount
                )

                update_dict = {
                    'name': row_symbol.name,
                    'sector': row_symbol.sector,
                    'industry': row_symbol.industry,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'adj_close': row['adj_close'],
                    'volume': row['volume'],
                    'change': change,
                    'adj_change': adj_change,
                    'adj_amount': adj_amount
                }

                upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['date', 'symbol'],
                    set_=update_dict
                )

                session.execute(upsert_stmt)

            session.commit()
            self.logger.info(f"{index} : {symbol} Stock data saved at {now.strftime('%Y-%m-%d %H:%M:%S')}")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error saving stock data for {symbol}: {e}")
        finally:
            session.close()


    # S&P 500 Data Daily Load
    def collect_stock_prices(self, now : datetime) -> None:
        self.logger.info(f"Collecting stock prices at {now.strftime('%Y-%m-%d %H:%M:%S')}")
        symbols = self.get_snp500_symbols()
        self.logger.info(f"Number of symbols retrieved: {len(symbols)}")
        if not symbols:
            self.logger.warning("No symbols to process. Skipping stock price collection.")
            return
        
        for index, symbol in enumerate(symbols):
            symbol, data = self.fetch_stock_data(symbol, now)
            if data is not None and not data.empty:
                self.save_stock_data(index, symbol, data, now)
        self.logger.info("Stock price collection completed")



    def collect_news(self):
        self.logger.info("Collecting news")
        try:
            webhook_url = self.config['webhook_url_perplexity']
            data = {"stock_list": ['dummy']}  # TODO: data 전송으로 s&p 500 리스트 모두 전송하는게 맞을지 검토 필요
            response = requests.post(webhook_url, json=data, timeout=10)
            response.raise_for_status()
            self.logger.info("Signal sent to Make.com successfully")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending signal to Make.com: {e}")
            raise


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
    def job(self) -> None:
        try:
            now = self.get_current_date()
            print('#'*50)
            print(now)
            print('#'*50)
            status = self.check_market_status(now)
            self.logger.info(f"Market status: {status}")
            if status == "BEFORE_OPEN":
                self.update_snp500_list()
                # self.collect_news()
                # self.send_signal_to_make()
            elif status == "AFTER_OPEN":
                # self.collect_news()
                self.collect_stock_prices(now)
                # stock_list = self.analyze_stock()
                # self.send_signal_to_make(stock_list)
        except Exception as e:
            self.logger.error(f"An error occurred during job execution: {e}")

    # 스케줄링
    def schedule_jobs(self) -> None:
        schedule.every().day.at("00:00").do(self.update_job_schedule)
        self.update_job_schedule()

    def update_job_schedule(self):
        now = self.get_current_date()
        if now.dst():           # 서머타임이면, 정규장 한국 시간 오후 10시 30분 ~ 오전 5시 / 프리마켓 오후 5시 / 애프터마켓 오전 8시
            before_time = "22:00" #"22:00"
            after_time = "15:47" # 05:30
        else:                   # 서머타임 아니면, 정규장 한국 시간 오후 11시 30분 ~ 오전 6시 / 프리마켓 오후 6시 / 애프터마켓 오전 9시
            before_time = "23:00"
            after_time = "06:30"

        schedule.clear('daily-jobs')
        schedule.every().day.at(before_time).do(self.job).tag('daily-jobs')
        schedule.every().day.at(after_time).do(self.job).tag('daily-jobs')

        self.logger.info(f"Jobs scheduled at {before_time} and {after_time}")


    def health_check(self):
        try:
            # 메모리 사용량 체크
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage = memory_info.rss / 1024 / 1024  # MB 단위로 변환

            # DB 연결 상태 체크
            session = self.Session()
            session.execute(text("SELECT 1"))
            session.close()

            # 디스크 사용량 체크
            disk_usage = psutil.disk_usage('/')
            disk_percent = disk_usage.percent

            self.logger.info(f"Health Check - Memory Usage: {memory_usage:.2f} MB, Disk Usage: {disk_percent:.2f}%")

            # 메모리나 디스크 사용량이 특정 임계값을 넘으면 경고 로그 출력
            if memory_usage > 1000:  # 1GB 이상 사용 시
                self.logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
            if disk_percent > 90:  # 디스크 사용량 90% 이상 시
                self.logger.warning(f"High disk usage detected: {disk_percent:.2f}%")

        except Exception as e:
            self.logger.error(f"Error during health check: {e}")

    # 작동 진입점
    def run(self) -> None:
        self.create_tables() # 불필요시 비활성화
        self.logger.info("Starting market data collector")
        self.schedule_jobs()

        last_health_check = time.time()
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)

                if time.time() - last_health_check > 3600:
                    self.health_check()
                    last_health_check = time.time()
        except Exception as e:
            self.logger.error(f"An error occurred in the main loop: {e}")
            time.sleep(60)  # 1분 대기 후 재시도
        finally:
            self.logger.info("Shutting down market data collector")



if __name__ == "__main__":
    collector = MarketDataCollector('config.yaml')
    collector.run()