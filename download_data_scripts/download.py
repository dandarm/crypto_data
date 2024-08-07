from typing import Any, Callable, Dict, List, Optional
from pathlib import Path
from Configuration import Configuration
from download_data_scripts.resolvers.exchange_resolver import ExchangeResolver
from download_data_scripts.pairlist_helpers import expand_pairlist
from download_data_scripts.data.history import (convert_trades_to_ohlcv, refresh_backtest_ohlcv_data,
                                    refresh_backtest_trades_data)

def setup_utils_configuration(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare the configuration for utils subcommands
    :param args: Cli args from Arguments()
    :return: Configuration
    """
    configuration = Configuration(args)
    config = configuration.get_config()

    # Ensure we do not use Exchange credentials
    #remove_credentials(config)
    #validate_config_consistency(config)

    return config


def start_download_data(args: Dict[str, Any]) -> None:
    config = setup_utils_configuration(args)
    pairs_not_available: List[str] = []

    exchange = ExchangeResolver.load_exchange(config['exchange']['name'], config, validate=False)
    # Manual validations of relevant settings
    exchange.validate_pairs(config['pairs'])
    expanded_pairs = expand_pairlist(config['pairs'], list(exchange.markets))

    print(f"About to download pairs: {expanded_pairs}, to {config['data_dir']}")

    if config.get('download_trades'):
        pairs_not_available = refresh_backtest_trades_data(exchange, pairs=expanded_pairs, datadir=Path(config['data_dir']),
                                                            timerange=None, erase=bool(config.get('erase')),
                                                            data_format=config['dataformat_trades'])
