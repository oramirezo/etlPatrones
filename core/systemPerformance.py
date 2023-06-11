import os
import threading
import time
import psutil

class systemPerformance(threading.Thread):
    def __init__(self, period=None):
        threading.Thread.__init__(self)
        self.period = period

    def run(self):
        #print(f'[ok] System performance monitor running...')
        #ramGB = self.get_ram_value()
        try:
            cpu_usage = psutil.cpu_percent()
            cpu_freq = round(psutil.cpu_freq().current / 1000.0, 3)
            ram_usage = round(psutil.virtual_memory().percent, 2)

            print(f'\033[94m> Performance monitor\033[0m')
            #print(f'> Performance monitor...')
            print(f'  CPU usage: {cpu_usage}%.')
            print(f'  CPU freq: {cpu_freq}GHz')
            print(f'  RAM usage: {ram_usage}%')
        except Exception as e:
            print(f'[error] System performance monitor. {e}')
        if self.period:
            time.sleep(self.period)

    def get_ram_value(self):
        try:
            memBytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            ramGB = memBytes / (1024. ** 3)
            return ramGB
        except Exception as e:
            print(f'[error] get_ram_value. {e}')
            return 0.0