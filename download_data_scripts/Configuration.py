from typing import Any, Callable, Dict, List, Optional
from pathlib import Path
import sys
import rapidjson
CONFIG_PARSE_MODE = rapidjson.PM_COMMENTS | rapidjson.PM_TRAILING_COMMAS

class Configuration:

    def __init__(self, args: Dict[str, Any]) -> None:
        self.args = args
        self.config: Optional[Dict[str, Any]] = None

    def get_config(self) -> Dict[str, Any]:
        """
        Return the config. Use this method to get the bot config
        :return: Dict: Bot config
        """
        if self.config is None:
            self.config = self.load_config()

        return self.config

    def load_config(self) -> Dict[str, Any]:
        """
        Extract information for sys.argv and load the bot configuration
        :return: Configuration dictionary
        """
        # Load all configs
        config: Dict[str, Any] = self.load_config_file(self.args.get("config", []))

        # Keep a copy of the original configuration file
        # config['original_config'] = deepcopy(config)
        # self._process_logging_options(config)
        # self._process_runmode(config)
        # self._process_common_options(config)
        # self._process_trading_options(config)
        # self._process_optimize_options(config)
        # self._process_plot_options(config)

        # Check if the exchange set by the user is supported
        # check_exchange(config, config.get('experimental', {}).get('block_bad_exchanges', True))

        self._resolve_pairs_list(config)

        # process_temporary_deprecated_settings(config)

        return config

    def load_config_file(self, path: str) -> Dict[str, Any]:
        """
        Loads a config file from the given path
        :param path: path as str
        :return: configuration as dictionary
        """
        try:
            # Read config from stdin if requested in the options
            with open(path) if path != '-' else sys.stdin as file:
                config = rapidjson.load(file, parse_mode=CONFIG_PARSE_MODE)
        except FileNotFoundError:
            raise Exception(
                f'Config file "{path}" not found!'
                ' Please create a config file or check whether it exists.')
        except rapidjson.JSONDecodeError as e:
            #err_range = log_config_error_range(path, str(e))
            raise Exception(
                f'{e}\n'
                f'Please verify the following segment of your configuration:\n'
                #if err_range else 'Please verify your configuration file for syntax errors.'
            )

        return config

    def load_file(self, path: Path) -> Dict[str, Any]:
        try:
            with path.open('r') as file:
                config = rapidjson.load(file, parse_mode=CONFIG_PARSE_MODE)
        except FileNotFoundError:
            raise Exception(f'File file "{path}" not found!')
        return config

    def _resolve_pairs_list(self, config: Dict[str, Any]) -> None:
        """
        Helper for download script.
        Takes first found:
        * -p (pairs argument)
        * --pairs-file
        * whitelist from config
        """

        if "pairs" in config:
            return

        if "pairs_file" in self.args and self.args["pairs_file"]:
            pairs_file = Path(self.args["pairs_file"])
            print(f'Reading pairs file "{pairs_file}".')
            # Download pairs from the pairs file if no config is specified
            # or if pairs file is specified explicitely
            if not pairs_file.exists():
                raise Exception(f'No pairs file found with path "{pairs_file}".')
            config['pairs'] = self.load_file(pairs_file)
            config['pairs'].sort()
            return

        if 'config' in self.args and self.args['config']:
            print("Using pairlist from configuration.")
            config['pairs'] = config.get('exchange', {}).get('pair_whitelist')
        else:
            # Fall back to /dl_path/pairs.json
            pairs_file = config['datadir'] / 'pairs.json'
            if pairs_file.exists():
                config['pairs'] = self.load_file(pairs_file)
                if 'pairs' in config:
                    config['pairs'].sort()


def setup_utils_configuration(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare the configuration for utils subcommands
    :param args: Cli args from Arguments()
    :return: Configuration
    """
    configuration = Configuration(args)
    config = configuration.get_config()

    # Ensure we do not use Exchange credentials
    # remove_credentials(config)
    # validate_config_consistency(config)

    return config