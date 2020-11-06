import logging
from datetime import datetime
from paymaster.structs.ppdataframe import PrepayDataFrame
from paymaster.loghandler.handler import PrepayLogger


def to_parquet(schema: str = None, log_path: str = None):
    try:
        start_time = datetime.now()
        base_logger = logging.getLogger(__name__)
        logger = PrepayLogger(base_logger, fh_path=log_path)
        logger.info('***Started*** at {}'.format(datetime.now()))
        frame = PrepayDataFrame(schema_yaml=schema)
        frame.read_csv()
        frame.to_parquet()
        end_time = datetime.now()
        logger.info('***Finished*** at {}'.format(end_time))
        logger.info('Wall time: {}'.format(end_time-start_time))
        return frame
    except Exception as e:
        raise e
