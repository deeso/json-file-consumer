import os
import shutil
import time
from multiprocessing import Queue, Process


class RmFilesTask(object):

    def __init__(self, poll_time=60):
        self.cmd_queue = Queue()
        self.out_queue = Queue()

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
            self.poll_time,
            self.cmd_queue,
            self.out_queue
        ]
        self.proc = Process(target=self.rm_files, args=args)
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
        self.cmd_queue.put(True)
        time.sleep(2*self.poll_time)
        if self.proc.is_alive():
            self.proc.terminate()
        self.proc.join()

    def add_filename(self, filename):
        if not os.path.isdir(filename):
            self.cmd_queue.put({'filename': filename})
        else:
            self.cmd_queue.put({'directory': filename})

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
    def rm_files(cls, poll_time, in_queue, out_queue):

        while True:
            d = cls.read_inqueue(in_queue)
            if cls.check_for_quit(d):
                break

            result = None
            if 'filename' in d:
                filename = d.get('filename', None)
                if filename is not None:
                    result = {'filename': filename}
                    try:
                        os.remove(filename)
                        result['removed'] = True
                    except:
                        result['removed'] = False
            elif 'directory' in d:
                directory = d.get('directory', None)
                if directory is not None:
                    result = {'directory': directory}
                    try:
                        shutil.rmtree(directory)
                        result['removed'] = True
                    except:
                        result['removed'] = False
            else:
                result = {'unknown': d, 'removed': False}

            out_queue.put(result)
            if in_queue.empty():
                time.sleep(poll_time)

    @classmethod
    def from_toml(cls, toml_dict):
        poll_time = toml_dict.get('poll_time', 20)
        return cls(poll_time)
