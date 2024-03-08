import csv
from datetime import datetime, date
from calendar import monthrange
from typing import List, Optional, Callable, Dict, Union

from pandas import DataFrame
from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange, Market, Conflict
from vnpy.trader.object import BarData, TickData, ContractData, HistoryRequest
from vnpy.trader.database import BaseDatabase, get_database, BarOverview, DB_TZ
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed, get_datafeeds
from vnpy.trader.utility import ZoneInfo, exchange_to_market

from ex_vnpy.object import BasicStockData, BasicIndexData

APP_NAME = "DataManager"


class ManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.database: BaseDatabase = get_database()
        self.datafeed: BaseDatafeed = get_datafeed()
        self.datafeeds: Dict[Market, BaseDatafeed] = get_datafeeds()

    def import_data_from_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        tz_name: str,
        datetime_head: str,
        open_head: str,
        high_head: str,
        low_head: str,
        close_head: str,
        volume_head: str,
        turnover_head: str,
        open_interest_head: str,
        datetime_format: str
    ) -> tuple:
        """"""
        with open(file_path, "rt") as f:
            buf: list = [line.replace("\0", "") for line in f]

        reader: csv.DictReader = csv.DictReader(buf, delimiter=",")

        bars: List[BarData] = []
        start: datetime = None
        count: int = 0
        tz = ZoneInfo(tz_name)

        for item in reader:
            if datetime_format:
                dt: datetime = datetime.strptime(item[datetime_head], datetime_format)
            else:
                dt: datetime = datetime.fromisoformat(item[datetime_head])
            dt = dt.replace(tzinfo=tz)

            turnover = item.get(turnover_head, 0)
            open_interest = item.get(open_interest_head, 0)

            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item[volume_head]),
                open_price=float(item[open_head]),
                high_price=float(item[high_head]),
                low_price=float(item[low_head]),
                close_price=float(item[close_head]),
                turnover=float(turnover),
                open_interest=float(open_interest),
                gateway_name="DB",
            )

            bars.append(bar)

            # do some statistics
            count += 1
            if not start:
                start = bar.datetime

        end: datetime = bar.datetime

        # insert into database
        self.database.save_bar_data(bars)

        return start, end, count

    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        bars: List[BarData] = self.load_bar_data(symbol, exchange, interval, start, end)

        fieldnames: list = [
            "symbol",
            "exchange",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "open_interest"
        ]

        try:
            with open(file_path, "w") as f:
                writer: csv.DictWriter = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                for bar in bars:
                    d: dict = {
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": bar.open_price,
                        "high": bar.high_price,
                        "low": bar.low_price,
                        "close": bar.close_price,
                        "turnover": bar.turnover,
                        "volume": bar.volume,
                        "open_interest": bar.open_interest,
                    }
                    writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self, symbol_id: int = None, symbol: str = None, stype: str = 'CS') -> List[BarOverview]:
        if symbol_id:
            return self.database.get_bar_overview(symbol_id=symbol_id)
        else:
            return self.database.get_bar_overview(symbol=symbol, stype=stype)

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bars: List[BarData] = self.database.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        count: int = self.database.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        return count

    def download_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end=None,
        conflict=Conflict.REPLACE,
        output: Callable=print
    ) -> int:
        """
        Query bar data from datafeed.
        """
        end = end if end else datetime.now(DB_TZ)
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end
        )

        datafeed = self.get_datafeed_by_exchange(exchange)
        data: List[BarData] = datafeed.query_bar_history(req, output)
        if data:
            self.database.save_bar_data(data, conflict=conflict)

        return len(data)

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        output: Callable,
        end=None
    ) -> int:
        """
        Query tick data from datafeed.
        """
        end = end if end else datetime.now(DB_TZ)
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=end
        )

        datafeed = self.get_datafeed_by_exchange(exchange)
        data: List[TickData] = datafeed.query_tick_history(req, output)

        if data:
            self.database.save_tick_data(data)
            return (len(data))

        return 0

    def get_stocks_list(self, markets: List[Market]) -> Dict[Market, List[BasicStockData]]:
        return self.database.get_basic_stock_data(markets)

    def get_index_list(self, markets: List[Market]) -> Dict[Market, List[BasicIndexData]]:
        return self.database.get_basic_index_data(markets)

    def save_bar_data(self, df: DataFrame, symbol: str, exchange: Exchange, interval: Interval, start: datetime, end: datetime):
        datafeed = self.get_datafeed_by_exchange(exchange)
        data = datafeed.handle_bar_data(df, symbol, exchange, interval, start, end)
        if data and len(data) > 0:
            self.database.save_bar_data(data)

    def get_datafeed_by_market(self, market: Market):
        return self.datafeeds[market]

    def get_datafeed_by_exchange(self, exchange: Exchange):
        market = exchange_to_market(exchange)
        return self.datafeeds[market]

    def log_operation(self, type: str, op_status: str, op_time: datetime, op_info: str = ""):
        self.database.save_operation_log(type, op_status, op_time, op_info)

    def save_capital_data(self, capital_data: List):
        self.database.save_capital_data(capital_data)

    def save_capital_flat_data(self, capital_data: List):
        self.database.save_capital_flat_data(capital_data)

    def get_capital_days_by_month(self, month_first_day: date):
        _, days_in_month = monthrange(month_first_day.year, month_first_day.month)
        month_last_day = date(month_first_day.year, month_first_day.month, days_in_month)
        return self.database.get_capital_days(month_first_day, month_last_day)

    def get_latest_statistic_date(self):
        return self.database.get_latest_statistic_date()

    def get_latest_op_info(self, op_type):
        return self.database.get_latest_op_info(op_type)

    def get_capital_data_by_month(self, month) -> List:
        return self.database.get_capital_data_by_month(month)

    def get_capital_flat_data_by_symbol(self, symbol_id, start_dt: datetime = None, end_dt: datetime = None) -> List:
        return self.database.get_capital_flat_data_by_symbol(symbol_id, start_dt, end_dt)
