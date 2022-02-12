import time
from loguru import logger


reminder_before_hg_offset = 0 # 24 hours before hg time
reminder_after_hg_offset = 10800  # 3 hours after hg time

min_hg_time = time.mktime(time.strptime('08:00', '%H:%M'))
max_hg_time = time.mktime(time.strptime('22:59', '%H:%M'))

utc_offset = -10800 # time_str comes in UTC+3


def get_reminder_before_hg_time_utc(time_str):
    return get_time_with_offset(time_str, reminder_before_hg_offset + utc_offset)


def get_reminder_after_hg_time_utc(time_str):
    return get_time_with_offset(time_str, reminder_after_hg_offset + utc_offset)


def get_time_with_offset(time_str, offset_secs):
    parsed_time = parse_time(time_str)
    if parsed_time is None:
        return None
    time_with_offset = time.localtime(time.mktime(parsed_time) + offset_secs)
    return time.strftime('%H:%M', time_with_offset)


def parse_time(time_str):
     try:
        parsed = time.strptime(time_str, '%H:%M')
        if time.mktime(parsed) < min_hg_time:
            logger.warning(f'hg starts too early: {time_str}')
            return None
        elif time.mktime(parsed) > max_hg_time:
            logger.warning(f'hg starts too late: {time_str}')
            return None
        return parsed
     except ValueError:
        logger.warning(f'Failed to parse time str: {time_str}')
        return None

