from datetime import datetime, time

def dateToTimestamp(value):
    value = datetime.combine(value, time())
    return datetimeToTimestamp(value)

def datetimeToTimestamp(value):
    return (value - datetime(1970, 1, 1)).total_seconds()