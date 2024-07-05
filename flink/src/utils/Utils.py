from pyflink.datastream.state import ValueStateDescriptor
import datetime
from pyflink.common.watermark_strategy import TimestampAssigner

class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(datetime.datetime.strptime(value['date'], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000)
