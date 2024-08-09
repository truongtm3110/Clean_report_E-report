import calendar
import logging
import time
import traceback
from datetime import datetime

logger = logging.getLogger(__name__)


def get_timestamp_from_datetime(datetime_obj):
    try:
        return int(datetime_obj.timestamp())
    except Exception as e:
        pass
    return None


def get_datetime_from_timestamp(timestamp):
    # logger.info('get_datetime_from_timestamp', timestamp)
    """
    Get date time from timestamp by milisecond or second unit
    :param timestamp: timestamp by milisecond or second unit
    :return: datetime
    """
    if timestamp is None or len(str(timestamp)) < 5:
        return None
    try:
        timestamp = int(timestamp)
        if len(str(timestamp)) < 13:
            return datetime.fromtimestamp(timestamp)
        else:
            return datetime.fromtimestamp(int(timestamp / 1000))
    except Exception as e:
        logger.warning(e)
    return None


def normalize_time_stamp(timestamp):
    # logger.info('get_datetime_from_timestamp', timestamp)
    """
    Get date time from timestamp by milisecond or second unit
    :param timestamp: timestamp by milisecond or second unit
    :return: timestamp by second
    """
    if timestamp is None or len(str(timestamp)) < 5:
        return None
    try:
        timestamp = int(timestamp)
        if len(str(timestamp)) < 13:
            return timestamp
        else:
            return int(timestamp / 1000)
    except Exception as e:
        logger.warning(e)
    return None


def get_timestamp_by_ms(timestamp):
    if timestamp is None:
        return None
    if len(str(timestamp)) < 13:
        return timestamp * 1000
    return timestamp


def get_timestamp_by_s(timestamp):
    if timestamp is None:
        return None
    if len(str(timestamp)) == 13:
        return timestamp // 1000
    return timestamp


def get_datetime_tz_from_timestamp(timestamp, timezone='Asia/Ho_Chi_Minh'):
    import pytz
    d = get_datetime_from_timestamp(timestamp)
    if d is None:
        return None

    # timezone = pytz.timezone(settings.TIME_ZONE)
    timezone = pytz.timezone(timezone)
    datetime_with_timezone = timezone.localize(d)
    # print(datetime_with_timezone)
    return datetime_with_timezone


def get_timestamp_now(should_timestamp_second=True):
    if should_timestamp_second:
        return int(datetime.now().timestamp())
    return int(datetime.now().timestamp() * 1000)


def get_timestamp_now_with_timezone(timezone='Asia/Ho_Chi_Minh', should_timestamp_second=True):
    date_time_now = get_date_time_now_with_timezone(timezone=timezone)
    if should_timestamp_second:
        return int(date_time_now.timestamp())
    return int(date_time_now.timestamp() * 1000)


def get_date_time_now_with_timezone(timezone='Asia/Ho_Chi_Minh'):
    return get_datetime_tz_from_timestamp(get_timestamp_now(), timezone)


def get_date_time_str_now_with_timezone(timezone='Asia/Ho_Chi_Minh', format="%Y%m%d_%H"):
    time_now = get_datetime_tz_from_timestamp(get_timestamp_now(), timezone)
    return time_now.strftime(format)


def get_str_iso_time_from_timestamp(timestamp, timezone='Asia/Ho_Chi_Minh'):
    import pytz
    # ts = os.path.getctime(timestamp)
    dt = datetime.fromtimestamp(timestamp, pytz.timezone(timezone))
    return dt.isoformat()


def convert_str_to_datetime_tz(time_str, format='%Y-%m-%d %H:%M:%S', timezone='Asia/Ho_Chi_Minh'):
    import pytz
    try:
        d = datetime.strptime(time_str, format)
        timezone = pytz.timezone(timezone)
        datetime_with_timezone = timezone.localize(d)
        return datetime_with_timezone
    except ValueError:
        pass
    return None


def get_datetime_now_with_timezone() -> datetime:
    import pytz
    return pytz.UTC.localize(datetime.utcnow())


def convert_str_to_datetime_and_change_tz(time_str, format='%Y-%m-%dT%H:%M:%S.%f%z',
                                          timezone='Asia/Ho_Chi_Minh'):
    import pytz
    try:
        d = datetime.strptime(time_str, format)
        timezone = pytz.timezone(timezone)
        new_datetime_with_timezone = d.astimezone(timezone)
        return new_datetime_with_timezone
    except ValueError:
        traceback.print_exc()
    return None


def convert_str_to_datetime(str, format='%Y-%m-%d %H:%M:%S'):
    if str is not None and len(str) > 0:
        try:
            return datetime.strptime(str, format)
        except ValueError:
            traceback.print_exc()
    return None


def convert_str_to_time(str, format='%H:%M:%S'):
    if str is not None and len(str) > 0:
        try:
            return time.strptime(str, format)
        except ValueError:
            pass
    return None


def convert_str_to_date(str_date, format='%Y-%m-%d'):
    date = None
    if str_date:
        try:
            date = datetime.strptime(str_date, format).date()
        except Exception as e:
            print("Invalid date:", str_date)
        return date


def convert_date_to_readable_str(date_obj):
    return date_obj.strftime("%d/%m/%Y")


def convert_datetime_to_data_version(input_datetime):
    return str(input_datetime.strftime("%Y%m%d_%H%M%S_%f")[:-3])


def get_date_current():
    date_current = datetime.now().strftime("%Y%m%d")
    # print(date_current)
    return date_current


def get_date_time_current():
    date_time_current = datetime.now().strftime("%Y%m%d_%H")
    # print(date_time_current )
    return date_time_current


def get_current_datetime_to_data_version():
    return convert_datetime_to_data_version(datetime.now())


def string_ago_to_datetime(str_days_ago, today=datetime.now()):
    from dateutil.relativedelta import relativedelta
    splitted = str_days_ago.split()
    if len(splitted) == 1 and splitted[0].lower() == 'today':
        return str(today.isoformat())
    elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
        date = today - relativedelta(days=1)
        return str(date.isoformat())
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = today - relativedelta(hours=int(splitted[0]))
        return str(date.date().isoformat())
    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = today - relativedelta(days=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = today - relativedelta(weeks=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = today - relativedelta(months=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = today - relativedelta(years=int(splitted[0]))
        return str(date.isoformat())
    else:
        return None


def convert_datetime_to_other_timezone(input_time: datetime, timezone='Asia/Ho_Chi_Minh'):
    import pytz
    timezone = pytz.timezone(timezone)
    if input_time.tzinfo:
        return input_time.astimezone(timezone)
    datetime_with_timezone = timezone.localize(input_time)
    return datetime_with_timezone


def get_lst_month_in_range(start_date, end_date, format='%Y%m%d'):
    from dateutil.relativedelta import relativedelta
    start_at = convert_str_to_datetime(str=start_date, format=format)
    end_at = convert_str_to_datetime(str=end_date, format=format)

    lst_month_range = []
    current = start_at
    while current <= end_at:
        month_start = current
        if month_start <= start_at:
            month_start = current.replace(day=1)
        month_end = month_start.replace(day=calendar.monthrange(current.year, current.month)[1])
        if month_end >= end_at:
            month_end = end_at
        current += relativedelta(months=1)
        lst_month_range.append({
            'start': month_start,
            'end': month_end,
        })
    return lst_month_range


def get_start_end_of_month(month_datetime: datetime):
    # convert_str_to_datetime(str=f'202208', format='%Y%m')
    month_start = month_datetime.replace(day=1)
    month_end = month_start.replace(day=calendar.monthrange(month_datetime.year, month_datetime.month)[1])
    return month_start, month_end


# current_milli_time = lambda: int(round(time.time() * 1000))

# print(get_datetime_from_timestamp(1557644420))
# print(get_str_iso_time_from_timestamp(1557644420))
if __name__ == '__main__':
    # string = '2018-12-04 17:16:58'
    # print(convert_str_to_datetime(string).timestamp())
    # print(convert_str_to_time("16:09:38"))
    # print(convert_str_to_datetime("2022-03-10", format='%Y-%m-%d'))
    # print(get_timestamp_now())
    # print(get_date_time_str_now_with_timezone(format="%m/%Y"))
    print(get_start_end_of_month(month_datetime=convert_str_to_datetime(str=f'202208', format='%Y%m')))
    # print(convert_str_to_datetime(str=f'202208', format='%Y%m'))
