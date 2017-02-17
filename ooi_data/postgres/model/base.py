# coding: utf-8
import datetime
from sqlalchemy import BIGINT
from sqlalchemy import FLOAT
from sqlalchemy import TypeDecorator
from sqlalchemy.ext.declarative import declarative_base

MetadataBase = declarative_base()
MonitorBase = declarative_base()


class UnixMillisTimestamp(TypeDecorator):
    impl = BIGINT
    UNIX_EPOCH = datetime.datetime(1970, 1, 1)

    def _total_millis(self, dt):
        return int((dt - self.UNIX_EPOCH).total_seconds() * 1000)

    def process_bind_param(self, value, dialect):
        return self._total_millis(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return datetime.datetime.utcfromtimestamp(value / 1000.0)


class NtpSecsTimestamp(TypeDecorator):
    impl = FLOAT
    NTP_EPOCH = datetime.datetime(1900, 1, 1)
    NTP_DELTA = (datetime.datetime(1970, 1, 1) - NTP_EPOCH).total_seconds()

    def _total_secs(self, dt):
        return (dt - self.NTP_EPOCH).total_seconds()

    def process_bind_param(self, value, dialect):
        return self._total_secs(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return datetime.datetime.utcfromtimestamp(value - self.NTP_DELTA)
