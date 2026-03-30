import time, threading

class Worker:
    def __init__(self, function):
        self.started_time = time.perf_counter()
        self.result = None
        self.error = None

        def run():
            try:
                self.result = function(self)
            except Exception  as e:
                self.error = e
        
        self.t = threading.Thread(target=run)
        self.t.start()
