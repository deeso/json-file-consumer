from filename_filter import Filter
import os
from multiprocessing import Process, Queue
import time


class DirCheckerTask(object):

    def __init__(self, target_dir, name_pattern=None, poll_time=30):
        self.target_dir = target_dir
        self.cmd_queue = Queue()
        self.out_queue = Queue()
        self.name_pattern = name_pattern

        self.poll_time = poll_time
        self.running = False
        self.proc = None

    def read_outqueue(self):
        filenames = []
        while not self.out_queue.empty():
            filenames.append(self.out_queue.get())
        return filenames

    def start(self):
        args = [
            self.target_dir,
            self.name_pattern,
            self.poll_time,
            self.cmd_queue,
            self.out_queue
        ]
        self.proc = Process(target=self.check_dir, args=args)
        self.proc.start()

    def is_running(self):
        if self.proc is None or not self.proc.is_alive():
            return False
        return True

    def set_queues(self, cmd_queue, out_queue):
        if not self.is_running():
            self.cmd_queue = cmd_queue
            self.out_queue = out_queue

    def stop(self):
        self.cmd_queue.put({'quit': True})
        time.sleep(2*self.poll_time)
        if self.proc.is_alive():
            self.proc.terminate()
        self.proc.join()

    def set_queues(self, cmd_queue, out_queue):
        if not self.is_running():
            self.cmd_queue = cmd_queue
            self.out_queue = out_queue

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
    def is_fileopened(cls, fname):
        if not os.path.exists(fname):
            return None
        try:
            os.rename(fname, fname)
            return False
        except:
            return True
        return None

    @classmethod
    def identify_files(cls, target_directory, ffilter=None):
        td = target_directory
        rfiles = [os.path.join(td, i) for i in os.listdir(td)]
        files = []

        for f in rfiles:

            if ffilter is None or ffilter.matches(f):
                if not cls.is_fileopened(f):
                    files.append(f)
        return sorted(files)

    @classmethod
    def check_dir(cls, target_dir, name_pattern,
                  poll_time, in_queue, out_queue):

        files_found = set()
        ffilter = None
        if name_pattern is not None:
            ffilter = Filter(name_pattern)

        d = cls.read_inqueue(in_queue)
        if cls.check_for_quit(d):
            return

        while True:
            d = cls.read_inqueue(in_queue)
            if cls.check_for_quit(d):
                break

            files = cls.identify_files(target_dir, ffilter)

            for f in files:
                if f in files_found:
                    continue
                files_found.add(f)
                out_queue.put({'filename': f})

                d = cls.read_inqueue(in_queue)
                if cls.check_for_quit(d):
                    break

            d = cls.read_inqueue(in_queue)
            if cls.check_for_quit(d):
                break
            time.sleep(poll_time)

    @classmethod
    def from_toml(cls, toml_dict):
        target_directory = toml_dict.get('target-directory', None)
        name_pattern = toml_dict.get('name-pattern', None)
        poll_time = toml_dict.get('poll-time', 20)
        return cls(target_directory, name_pattern, poll_time)
