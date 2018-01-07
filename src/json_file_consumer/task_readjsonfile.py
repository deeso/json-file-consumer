import time
from multiprocessing import Queue, Process
import json


class ReadJsonFileTask(object):

    def __init__(self, poll_time=60):
        self.cmd_queue = Queue()
        self.out_queue = Queue()

        self.poll_time = poll_time
        self.proc = None

    def read_outqueue(self):
        filenames = []
        while not self.out_queue.empty():
            filenames.append(self.out_queue.get())
        return filenames

    def start(self):
        args = [
            self.poll_time,
            self.cmd_queue,
            self.out_queue
        ]
        self.proc = Process(target=self.read_json_file, args=args)
        self.proc.start()

    def is_running(self):
        if self.proc is None or not self.proc.is_alive():
            return False
        return True

    def set_queues(self, cmd_queue, out_queue):
        if not self.is_running():
            self.cmd_queue = cmd_queue
            self.out_queue = out_queue

    def add_filename(self, filename):
        self.cmd_queue.put({'filename': filename})

    def stop(self):
        self.cmd_queue.put({'quit': True})
        time.sleep(2*self.poll_time)
        if self.proc.is_alive():
            self.proc.terminate()
        self.proc.join()

    @classmethod
    def read_inqueue(cls, queue):
        if queue.empty():
            return None
        return queue.get()

    @classmethod
    def check_for_quit(cls, json_data):
        quit = False
        if json_data is None:
            return quit

        if 'quit' in json_data:
            quit = True

        return quit

    @classmethod
    def read_json_file(cls, poll_time, in_queue, out_queue):

        while True:
            d = cls.read_inqueue(in_queue)
            if cls.check_for_quit(d):
                break

            if 'filename' in d:
                filename = d.get('filename', None)

                with open(filename) as infile:
                    json_lines = []
                    for line in infile.lines():
                        try:
                            data = json.loads(line)
                            json_lines.append(data)
                            if len(json_lines) > 200:
                                out_queue.put({'json_lines': json_lines})
                                json_lines = []
                        except:
                            pass

                out_queue.put({'json_lines': json_lines})

            if in_queue.empty():
                time.sleep(poll_time)

    @classmethod
    def from_toml(cls, toml_dict):
        poll_time = toml_dict.get('poll_time', 20)
        return cls(poll_time)
