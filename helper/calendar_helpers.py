import calendar


def get_all_month_between(start_month_str: str, end_month_str: str) -> list[str]:
    start_year = int(start_month_str[:4])
    end_year = int(end_month_str[:4])
    start_month = int(start_month_str[-2:])
    end_month = int(end_month_str[-2:])
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(start_month if year == start_year else 1,
                           end_month + 1 if year == end_year else 13):
            months.append(f"{year}{str(month).zfill(2)}")
    return months


def get_end_field_and_start_field(month_str: str, end_date, type_str: str):
    year = int(month_str[:4])
    month = int(month_str[-2:])
    end_year = int(end_date[:4])
    end_month = int(end_date[4:6])
    previous_month = month - 1 if month > 1 else 12
    previous_year = year - 1 if month == 1 else year
    type_field = 'order' if type_str == 'sale' else 'revenue'
    _, start_day = calendar.monthrange(year, previous_month)
    if year == end_year and month == end_month:
        return (f"{type_field}_history_{previous_year}{str(previous_month).zfill(2)}{str(start_day).zfill(2)}",
                f"{type_field}_history_{end_date}")
    else:
        _, end_day = calendar.monthrange(year, month)
        return (f"{type_field}_history_{previous_year}{str(previous_month).zfill(2)}{str(start_day).zfill(2)}",
                f"{type_field}_history_{year}{str(month).zfill(2)}{str(end_day).zfill(2)}")
