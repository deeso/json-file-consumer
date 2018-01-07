import logging
import argparse
import sys

SALT = 'salt'
KEY = 'this is a default key but not the best idea!'

CONFIG="config.toml"

parser = argparse.ArgumentParser(description='Run the json-file-consumer and monitor.')

parser.add_argument('-config', type=str, default=CONFIG,
                    help='configuration file for the service')

V = 'log levels: INFO: %d, DEBUG: %d, WARRNING: %d' % (logging.INFO,
                                                       logging.DEBUG,
                                                       logging.WARNING)
parser.add_argument('-log_level', type=int, default=logging.DEBUG,
                    help=V)

parser.add_argument('-encrypt_key', type=str, default=logging.DEBUG,
                    help=V)

if __name__ == "__main__":
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)

    clownsvc = ClownFactory.parse(args.config)

    try:
        logging.debug("Starting the syslog listener")
        service.serve_forever(poll_interval=0.5)
    except (IOError, SystemExit):
        raise
    except KeyboardInterrupt:
        raise
