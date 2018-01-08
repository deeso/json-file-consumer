from task_blox import TASK_BLOX_MAPPER
from multiprocessing import Queue
from threading import Thread
import os
import time

EVE_PATTERN = '.*eve-\d{4}-\d{2}-\d{2}-\d{2}:\d{2}\.json'


class ConsumerService(object):

    def __init__(self, dircheckers=[], jsonfilereaders=[],
                 rmfiles=None, elksubmitjsons=[], poll_time=20):

        self.poll_time = poll_time
        self.dircheckers = dircheckers

        if len(dircheckers) == 0:
            raise Exception("Directory checking tasks is need to be useful")

        # for round robin dispatch
        self.jfr_pos = 0
        self.l_jfr_pos = len(jsonfilereaders)
        self.jsonfilereaders = jsonfilereaders
        if len(dircheckers) == 0:
            raise Exception("JSON File readers tasks is need to be useful")

        self.rmfiles = rmfiles

        # for round robin dispatch
        self.esj_pos = 0
        self.elksubmitjsons = elksubmitjsons
        if len(elksubmitjsons) == 0:
            raise Exception("Elk submit jsons tasks is need to be useful")

        # data queue for tasks
        self.files_to_read = Queue()
        self.json_to_send = Queue()

        self.keep_running = False
        self.dirchecker_poll_thread = None
        self.jsonreader_poll_thread = None
        self.rmfiles_poll_thread = None
        self.elksubmit_poll_thread = None

    def start_dircheckers(self):
        for obj in self.dircheckers:
            obj.start()

    def start_jsonfilereaders(self):
        for obj in self.jsonfilereaders:
            obj.start()

    def start_elksubmitjsons(self):
        for obj in self.elksubmitjsons:
            obj.start()

    def start_rmfiles(self):
        if self.rmfiles is not None:
            self.rmfiles.start()

    def stop_dircheckers(self):
        for obj in self.dircheckers:
            obj.stop()

    def stop_jsonfilereaders(self):
        for obj in self.jsonfilereaders:
            obj.stop()

    def stop_elksubmitjsons(self):
        for obj in self.elksubmitjsons:
            obj.stop()

    def stop_rmfiles(self):
        if self.rmfiles is not None:
            self.rmfiles.stop()

    def dirchecker_poll(self):
        while self.keep_running:
            data = self.read_dircheckers_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
            for d in data:
                if 'filename' in d and os.path.isfile(d['filename']):
                        self.add_file_jsonfilereader(d['filename'])
                        break

    def add_file_jsonfilereader(self, filename):
        jfr = self.jsonfilereaders[self.jfr_pos % self.l_jfr_pos]
        jfr.add_filename(filename)
        self.jfr_pos += 1

    def jsonfilereaders_poll(self):

        while self.keep_running:
            data = self.read_jsonfilereaders_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
            for d in data:
                json_lines = d['json_lines'] if 'json_lines' in d else None
                filename = d['filename'] if 'filename' in d else None
                status = d['status'] if 'status' in d else None

                if self.rmfiles is not None and status == 'complete':
                    self.rmfiles.add_filename(filename)

                if len(json_lines) > 0:
                    self.add_json_line_elksubmitjson(json_lines)

    def rmfiles_poll(self):
        while self.keep_running:
            data = self.read_rmfiles_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
            for d in data:
                pass

    def add_json_line_elksubmitjson(self, json_lines):
        for esj in self.elksubmitjsons:
            esj.add_json_datas(json_lines)

    def elksubmitjson_poll(self):
        while self.keep_running:
            data = self.read_elksubmitjson_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
            for d in data:
                pass

    def start(self):
        self.keep_running = True
        self.start_dircheckers()
        self.start_jsonfilereaders()
        self.start_elksubmitjsons()
        self.start_rmfiles()

        t = Thread(target=self.dirchecker_poll)
        self.dirchecker_poll_thread = t
        t.start()
        t = Thread(target=self.jsonfilereaders_poll)
        self.jsonreader_poll_thread = t
        t.start()
        if self.rmfiles is not None:
            t = Thread(target=self.rmfiles_poll)
            self.rmfiles_poll_thread = t
            t.start()

        t = Thread(target=self.elksubmitjson_poll)
        self.elksubmit_poll_thread = t
        t.start()

    def stop(self):
        self.stop_dircheckers()
        self.stop_jsonfilereaders()
        self.stop_elksubmitjsons()
        self.stop_rmfiles()
        self.keep_running = False

    def read_jsonfilereaders_output(self):
        out_data = []
        for obj in self.jsonfilereaders:
            while True:
                data = obj.read_outqueue()
                if len(data) == 0:
                    break
                out_data = out_data + data
        return out_data

    def read_dircheckers_output(self):
        out_data = []
        for obj in self.dircheckers:
            while True:
                data = obj.read_outqueue()
                if len(data) == 0:
                    break
                out_data = out_data + data
        return out_data

    def read_rmfiles_output(self):
        out_data = []
        if self.rmfiles is None:
            return out_data

        obj = self.rmfiles
        while True:
            data = obj.read_outqueue()
            if len(data) == 0:
                break
            out_data = out_data + data
        return out_data

    def read_elksubmitjson_output(self):
        out_data = []
        for obj in self.elksubmitjsons:
            while True:
                data = obj.read_outqueue()
                if len(data) == 0:
                    break
                out_data = out_data + data
        return out_data

    @classmethod
    def parse_toml(cls, toml_dict):

        cs_toml = toml_dict
        if 'json-file-consumer' in toml_dict:
            cs_toml = toml_dict.get('json-file-consumer')

        poll_time = cs_toml.get('poll-time', 20)
        dircheckers = []
        jsonfilereaders = []
        rmfiles = None
        elksubmitjsons = []

        cls = TASK_BLOX_MAPPER.get('dirchecker')
        blocks = cs_toml.get('dircheckers')
        for block in blocks.values():
            t = block.get('task')
            if t != cls.key():
                continue
            dc = cls(block)
            dircheckers.append(dc)

        cls = TASK_BLOX_MAPPER.get('jsonfilereader')
        blocks = cs_toml.get('jsonfilereaders')
        for block in blocks.values():
            t = block.get('task')
            if t != cls.key():
                continue
            dc = cls(block)
            jsonfilereaders.append(dc)

        cls = TASK_BLOX_MAPPER.get('elksubmitjson')
        blocks = cs_toml.get('elksubmitjsons')
        for block in blocks.values():
            t = block.get('task')
            if t != cls.key():
                continue
            dc = cls(block)
            elksubmitjsons.append(dc)

        cls = TASK_BLOX_MAPPER.get('rmfiles')
        block = cs_toml.get('rmfiles', None)
        if block is not None:
            rmfiles = cls(block)

        kargs = {
            'dircheckers': dircheckers,
            'jsonfilereaders': jsonfilereaders,
            'rmfiles': rmfiles,
            'elksubmitjsons': elksubmitjsons,
            'poll_time': poll_time,
        }

        return cls(**kargs)
