from json_file_consumer.consumer_service import ConsumerService
import logging
import argparse
import sys
import toml


CONFIG = "config.toml"
description = 'Run the json-file-consumer and monitor.'
parser = argparse.ArgumentParser(description=description)

parser.add_argument('-config', type=str, default=CONFIG,
                    help='configuration file for the service')

V = 'log levels: INFO: %d, DEBUG: %d, WARRNING: %d' % (logging.INFO,
                                                       logging.DEBUG,
                                                       logging.WARNING)
parser.add_argument('-log_level', type=int, default=logging.DEBUG,
                    help=V)

parser.add_argument('-config', type=str, default=None)

if __name__ == "__main__":
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)

    if args.config is None:
        raise Exception("A path to the config file is required")

    toml_dict = toml.load(open(args.config))
    csvc = ConsumerService.parse_toml(toml_dict)

    try:
        logging.debug("Starting the syslog listener")
        csvc.run_forever()
    except (IOError, SystemExit):
        csvc.stop()
    except KeyboardInterrupt:
        csvc.stop()
