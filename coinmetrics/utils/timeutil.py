from collections import deque
from datetime import datetime, timedelta
from typing import Any, List


def datetimeToTimestamp(value: datetime) -> float:
    return (value - datetime(1970, 1, 1)).total_seconds()


def datetimeFromMilliseconds(value: int) -> datetime:
    seconds = value // 1000
    milliseconds = value % 1000
    return datetime.utcfromtimestamp(seconds) + timedelta(milliseconds=milliseconds)


def alignDateToInterval(dt: datetime, interval: timedelta) -> datetime:
    intervalCount = datetimeToIntervalCount(dt, interval)
    return datetime.utcfromtimestamp(intervalCount * int(interval.total_seconds()))


def datetimeToIntervalCount(dt: datetime, interval: timedelta) -> int:
    dateSeconds = int(datetimeToTimestamp(dt))
    intervalSeconds = int(interval.total_seconds())
    return dateSeconds // intervalSeconds


class Timeline(object):

    def __init__(self, maxAgeInSeconds: float):
        self._events = deque()
        self._maxAge = maxAgeInSeconds
        assert self._maxAge > 0, "maximum age must be positive"

    def add(self, value: Any):
        now = datetime.now()
        self._events.append((now, value))
        while now - self._events[0][0] > self._maxAge:
            self._events.popleft()

    def getAllYoungerThan(self, ageInSeconds: float) -> List[Any]:
        result = []
        now = datetime.now()
        maxAgeTimedelta = timedelta(seconds=ageInSeconds)
        for eventTime, eventValue in reversed(self._events):
            if now - eventTime <= maxAgeTimedelta:
                result.append(eventValue)
            else:
                break
        return result
