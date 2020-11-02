import logging
from logging.handlers import RotatingFileHandler
import os
import socket

class PrepayLogger(logging.LoggerAdapter):
    def __init__(self, logger:logging.Logger, level=logging.INFO,
                 fh_path='/var/log/prepayment.log'):
        user = os.getlogin()
        hostname = socket.gethostname()
        extra = {'user': user, 'hostname': hostname}
        formatter = logging.Formatter(
            '%(hostname)s|%(user)s|%(asctime)s|%(levelname)s|'
            '%(name)s|%(funcName)s|%(message)s',"%Y-%m-%d %H:%M:%S"
        )
        fh = RotatingFileHandler(fh_path)
        fh.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(logging.CRITICAL)
        ch.setFormatter(formatter)
        self.fh = fh
        self.ch = ch
        logger.addHandler(self.fh)
        logger.addHandler(self.ch)
        super(PrepayLogger, self).__init__(logger, extra)
        self.setLevel(level)