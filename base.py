import time


class IntervalCaller:
    def __init__(self, func):
        self.func = func
        self.INTERVAL = 2
        self.last_time = self.now()
        self.start_time = self.now()

    @staticmethod
    def now():
        return round(time.time())

    def on_yield(self, *args, **kwargs):
        now = self.now()
        if (now - self.last_time) >= self.INTERVAL:
            elapse = now - self.start_time
            print(f'[{elapse}] ', end='')
            self.func(*args, **kwargs)
            self.last_time = now


def interval_call(func):
    caller = IntervalCaller(func)

    def wrapper(*args, **kwargs):
        return caller.on_yield(*args, **kwargs)
    return wrapper
