from typing import Any, Callable, Dict, List, Optional
import rapidjson
CONFIG_PARSE_MODE = rapidjson.PM_COMMENTS | rapidjson.PM_TRAILING_COMMAS

from download import start_download_data


def main():
    args = {'config': 'config.json'}
    start_download_data(args)

if __name__ == '__main__':
    main()