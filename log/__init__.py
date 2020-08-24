import logging.config
from logging import getLogger
import sys
import os
# import progressbar
import queue


bundle_path = getattr(sys, '_MEIPASS', os.path.abspath(os.path.dirname(__file__)))
log_file = os.path.join(bundle_path, 'logging.conf')
# progressbar.streams.wrap_stderr()
logging.config.fileConfig(log_file)
logger = getLogger('TrinityJobImport')


class QueueHandler(logging.Handler):

    def __init__(self):
        super().__init__()
        self.log_queue = queue.Queue()

    def emit(self, record):
        # put a formatted message to log_queue
        formatter = logging.Formatter(fmt='[%(levelname)s] %(asctime)s (%(name)s) %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S',
                                      )
        self.setFormatter(formatter)
        self.log_queue.put((self.format(record), record.levelname))


# Initializing Queue Instance for other logger to use
queue_handler = QueueHandler()
logger.addHandler(queue_handler)


def __add_option(parser):
    levels = ('INFO', 'WARNING', 'DEBUG', 'CRITICAL')
    parser.add_argument('--log-level',
                        choices=levels, metavar='LEVEL',
                        default='INFO',
                        dest='loglevel',
                        help='Amount of Detail on console messages. '
                             'LEVEL could be one of {}, (default: {})'.format(', '.join(levels), 'INFO'))


def __process_option(parser, opts):
    try:
        level = getattr(logging, opts.loglevel.upper())
    except ValueError:
        parser.error('Unknown Log Level : {}'.format(opts.loglevel))
    else:
        print('Set Log Level to {}'.format(opts.loglevel))
        logger.setLevel(level)


if __name__ == '__main__':
    pass
