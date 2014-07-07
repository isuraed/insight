import time

class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.stop_time = self.start_time

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.stop_time = time.time()

    def elapsed(self):
        return float('{:.3f}'.format(self.stop_time - self.start_time))
