import logging
import re
import io
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
from pandas import DataFrame, to_datetime

from download_data_scripts import misc
#from download_data_scripts.configuration import TimeRange
from download_data_scripts.constants import DEFAULT_DATAFRAME_COLUMNS, ListPairsWithTimeframes, TradeList
from download_data_scripts.data.converter import trades_dict_to_list

from .idatahandler import IDataHandler


logger = logging.getLogger(__name__)


class CSVDataHandler(IDataHandler):

    _use_zip = False
    _columns = DEFAULT_DATAFRAME_COLUMNS

    @classmethod
    def ohlcv_get_available_data(cls, datadir: Path) -> ListPairsWithTimeframes:
        """
        Returns a list of all pairs with ohlcv data available in this datadir
        :param datadir: Directory to search for ohlcv files
        :return: List of Tuples of (pair, timeframe)
        """
        _tmp = [re.search(r'^([a-zA-Z_]+)\-(\d+\S+)(?=.csv)', p.name)
                for p in datadir.glob(f"*.{cls._get_file_extension()}")]
        return [(match[1].replace('_', '/'), match[2]) for match in _tmp
                if match and len(match.groups()) > 1]

    @classmethod
    def ohlcv_get_pairs(cls, datadir: Path, timeframe: str) -> List[str]:
        """
        Returns a list of all pairs with ohlcv data available in this datadir
        for the specified timeframe
        :param datadir: Directory to search for ohlcv files
        :param timeframe: Timeframe to search pairs for
        :return: List of Pairs
        """

        _tmp = [re.search(r'^(\S+)(?=\-' + timeframe + '.json)', p.name)
                for p in datadir.glob(f"*{timeframe}.{cls._get_file_extension()}")]
        # Check if regex found something and only return these results
        return [match[0].replace('_', '/') for match in _tmp if match]

    def ohlcv_store(self, pair: str, timeframe: str, data: DataFrame) -> None:
        """
        Store data in CSV format "values".
            format looks as follows:
            [[<date>,<open>,<high>,<low>,<close>]]
        :param pair: Pair - used to generate filename
        :timeframe: Timeframe - used to generate filename
        :data: Dataframe containing OHLCV data
        :return: None
        """
        filename = self._pair_data_filename(self._datadir, pair, timeframe)
        _data = data.copy()
        # Convert date to int
        _data['date'] = _data['date'].astype(np.int64) // 1000 // 1000

        # Reset index, select only appropriate columns and save as CSV
        _data.reset_index(drop=True).loc[:, self._columns].to_json(
            filename, sep=",",
            compression='gzip' if self._use_zip else None)

    # nuova  per  gestire l'input CSV
    def _ohlcv_load(self, pair: str, timeframe: str,
                    timerange, #: Optional[TimeRange] = None,
                    ) -> DataFrame:
        """
        Internal method used to load data for one pair from disk.
        Implements the loading and conversion to a Pandas dataframe.
        Timerange trimming and dataframe validation happens outside of this method.
        :param pair: Pair to load data
        :param timeframe: Timeframe (e.g. "5m")
        :param timerange: Limit data to be loaded to this timerange.
                        Optionally implemented by subclasses to avoid loading
                        all data where possible.
        :return: DataFrame with ohlcv data, or empty DataFrame
        """
        filename = self._pair_data_filename(self._datadir, pair, timeframe)
        if not filename.exists():
            return DataFrame(columns=self._columns)
        try:
            pairdata = pd.read_csv(filename)
            # la colonna timestamp non è uguale a self._columns... . lo rendo uguale per sicurezza
            pairdata.rename(columns={"timestamp":"date"}, inplace=True)
            pairdata = pairdata[self._columns]
        except ValueError:
            logger.error(f"Could not load data for {pair}.")
            return DataFrame(columns=self._columns)

        pairdata = pairdata.astype(dtype={'open': 'float', 'high': 'float',
                                          'low': 'float', 'close': 'float', 'volume': 'float'})
        pairdata['date'] = to_datetime(pairdata['date'],
                                       # unit='ms',
                                       utc=True,
                                       infer_datetime_format=True)
        return pairdata

    def ohlcv_purge(self, pair: str, timeframe: str) -> bool:
        """
        Remove data for this pair
        :param pair: Delete data for this pair.
        :param timeframe: Timeframe (e.g. "5m")
        :return: True when deleted, false if file did not exist.
        """
        filename = self._pair_data_filename(self._datadir, pair, timeframe)
        if filename.exists():
            filename.unlink()
            return True
        return False

    def ohlcv_append(self, pair: str, timeframe: str, data: DataFrame) -> None:
        """
        Append data to existing data structures
        :param pair: Pair
        :param timeframe: Timeframe this ohlcv data is for
        :param data: Data to append.
        """
        raise NotImplementedError()

    @classmethod
    def trades_get_pairs(cls, datadir: Path) -> List[str]:
        """
        Returns a list of all pairs for which trade data is available in this
        :param datadir: Directory to search for ohlcv files
        :return: List of Pairs
        """
        _tmp = [re.search(r'^(\S+)(?=\-trades.json)', p.name)
                for p in datadir.glob(f"*trades.{cls._get_file_extension()}")]
        # Check if regex found something and only return these results to avoid exceptions.
        return [match[0].replace('_', '/') for match in _tmp if match]

    def trades_store(self, pair: str, data: TradeList) -> None:
        """
        Store trades data (list of Dicts) to file
        :param pair: Pair - used for filename
        :param data: List of Lists containing trade data,
                     column sequence as in DEFAULT_TRADES_COLUMNS
        """
        filename = self._pair_trades_filename(self._datadir, pair)
        #filename = self.renamefile(filename) # / "revised.csv"
        #misc.file_dump_json(filename, data, is_zip=self._use_zip)

        # Convert list of lists to DataFrame
        df = pd.DataFrame(data, columns=['timestamp', 'trade_id', 'null', 'type', 'price', 'amount', 'value'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        # Drop the 'null' column if it's not needed
        #df.drop(columns=['null'], inplace=True)

        # Append to CSV, create if file does not exist
        if filename.exists():
            df.to_csv(filename, mode='a', header=False, index=False)
        else:
            df.to_csv(filename, mode='w', header=True, index=False)

    def trades_append(self, pair: str, data: TradeList):
        """
        Append data to existing files
        :param pair: Pair - used for filename
        :param data: List of Lists containing trade data,
                     column sequence as in DEFAULT_TRADES_COLUMNS
        """
        raise NotImplementedError()

    def _trades_load(self, pair: str, timerange): #Optional[TimeRange] = None) -> TradeList:
        """
        Load a pair from file, either .json.gz or .json
        # TODO: respect timerange ...
        :param pair: Load trades for this pair
        :param timerange: Timerange to load trades for - currently not implemented
        :return: List of trades
        """
        filename = self._pair_trades_filename(self._datadir, pair)
        #tradesdata = misc.file_load_json(filename)

        if not filename.exists():
            return pd.DataFrame()  # Return an empty DataFrame if the file does not exist

        # Load the CSV file into a DataFrame
        #df = pd.read_csv(filename, parse_dates=['timestamp'], infer_datetime_format=True)
        df = self.load_only_last_row(filename)

        # Optional: filter data based on the timerange
        #if timerange:
        #    df = df[(df['timestamp'] >= timerange.start) & (df['timestamp'] <= timerange.end)]

        # Convert DataFrame to list of dictionaries (TradeList)
        #trade_list = df.to_dict('records')
        return df

        # if isinstance(tradesdata[0], dict):
        #     # Convert trades dict to list
        #     logger.info("Old trades format detected - converting")
        #     tradesdata = trades_dict_to_list(tradesdata)
        #     pass
        # return tradesdata

    def load_only_last_row(self, filename):
        # Leggi la prima riga per ottenere i titoli delle colonne
        with open(filename, 'r') as f:
            column_names = f.readline().strip().split(',')

        # Apri il file in modalità 'rb' (read binary) e posiziona il puntatore alla fine
        with open(filename, 'rb') as f:
            # Vai alla fine del file
            f.seek(-2, 2)
            # Muoviti indietro fino a trovare una nuova riga
            while f.read(1) != b'\n':
                f.seek(-2, 1)
            # Leggi l'ultima riga
            last_line = f.readline().decode()

        # Carica l'ultima riga in un DataFrame
        df_last_row = pd.read_csv(io.StringIO(last_line), header=None)
        df_last_row.columns = column_names
        # Effettua il parsing della colonna 'timestamp' come datetime
        df_last_row['timestamp'] = pd.to_datetime(df_last_row['timestamp'], infer_datetime_format=True)

        return df_last_row

    def trades_purge(self, pair: str) -> bool:
        """
        Remove data for this pair
        :param pair: Delete data for this pair.
        :return: True when deleted, false if file did not exist.
        """
        filename = self._pair_trades_filename(self._datadir, pair)
        if filename.exists():
            filename.unlink()
            return True
        return False

    @classmethod
    def _pair_data_filename(cls, datadir: Path, pair: str, timeframe: str) -> Path:
        pair_s = misc.pair_to_filename(pair)
        filename = datadir.joinpath(f'{pair_s}-{timeframe}.{cls._get_file_extension()}')
        return filename

    @classmethod
    def _get_file_extension(cls):
        return "csv.gz" if cls._use_zip else "csv"

    @classmethod
    def _pair_trades_filename(cls, datadir: Path, pair: str) -> Path:
        pair_s = misc.pair_to_filename(pair)
        filename = datadir.joinpath(f'{pair_s}-trades.{cls._get_file_extension()}')
        return filename

    def renamefile(self, filepath):
        #filepath.rename(filepath.with_name(f"{filepath.stem.replace('re', 'un')}{filepath.suffix}"))
        return filepath.rename(filepath.with_name(f"{filepath.stem}_revised_{filepath.suffix}"))
