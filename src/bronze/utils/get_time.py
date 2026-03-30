from datetime import datetime, timedelta

def get_date_str_today():
    return f"{datetime.now().date()}"

def _get_date_time_block(date_time: datetime) -> str:
    def round_hour_to_block_4(hour: int) -> int:
        return hour - hour % 4

    block_hour = round_hour_to_block_4(date_time.hour)
    block_hour_str = f"{block_hour:02d}"

    return f"{date_time.date()}T{block_hour_str}:00"


def get_today_date_time_block():
    return _get_date_time_block(datetime.now())


def get_yesterday_date_time_block():
    return _get_date_time_block(datetime.now() - timedelta(days=1))
