import sys
import logging
import requests

if __name__ == "__main__":
    result = 0

    LOG_LEVEL = 'DEBUG'  # if args.debug else 'INFO'
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(levelname)s: %(message)s', stream=sys.stdout)

    http_code = requests.get("http://simple-kafka-broker-default:9606").status_code
    if http_code != 200:
        result = 1

    sys.exit(result)
