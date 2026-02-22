from __future__ import annotations

from datetime import date, timedelta


def is_third_friday(d: date) -> bool:
    if d.weekday() != 4:
        return False
    first_day = d.replace(day=1)
    first_friday_offset = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=first_friday_offset)
    third_friday = first_friday + timedelta(days=14)
    return d == third_friday
