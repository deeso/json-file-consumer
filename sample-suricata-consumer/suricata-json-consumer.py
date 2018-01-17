from json_file_consumer.consumer_service import JsonConsumerService
from json_file_consumer import logger
import argparse
import os


CONFIG = "config.toml"
description = 'Run the json-file-consumer and monitor for suricata'
parser = argparse.ArgumentParser(description=description)

parser.add_argument('-config', type=str, default=None,
                    help='configuration file for the service')


if __name__ == "__main__":
    args = parser.parse_args()

    if args.config is None:
        raise Exception("A path to the config file is required")
    elif args.config:
        try:
            os.stat(args.config)
        except:
            raise Exception("Invalid path to the config file is required")

    csvc = JsonConsumerService.parse_toml_file(args.config)

    try:
        logger.debug(name=csvc.key()).info("Starting the syslog listener")
        csvc.run_forever()
    except (IOError, SystemExit):
        csvc.stop()
    except KeyboardInterrupt:
        csvc.stop()
