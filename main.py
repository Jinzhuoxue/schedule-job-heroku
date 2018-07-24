import schedule
import time

from app import (get_hist_data, get_index)

if __name__ == '__main__':
    schedule.every(1).day.do(get_index)

    get_index()
    while True:
        schedule.run_pending()
        time.sleep(1)