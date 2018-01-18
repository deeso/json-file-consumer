from threading import Thread
import os
import time
import toml
import logging

from json_file_consumer import logger
from task_blox import TASK_BLOX_MAPPER
from multiprocessing import Queue


class JsonConsumerService(object):
    KEY = 'json-consumer-service'

    @classmethod
    def key(cls):
        return cls.KEY.lower()

    def __init__(self, dircheckers=[], jsonfilereaders=[],
                 rmfiles=None, elksubmitjsons=[], poll_time=20,
                 log_level=logging.DEBUG, log_name=KEY.lower()):

        logger.init_logger(name=log_name, log_level=log_level)
        self.poll_time = poll_time
        self.dircheckers = dircheckers

        # if len(dircheckers) == 0:
        #     raise Exception("Directory checking tasks is need to be useful")

        # for round robin dispatch
        self.jfr_pos = 0
        self.l_jfr_pos = len(jsonfilereaders)
        self.jsonfilereaders = jsonfilereaders
        # if len(dircheckers) == 0:
        #     raise Exception("JSON File readers tasks is need to be useful")

        self.rmfiles = rmfiles

        # for round robin dispatch
        self.esj_pos = 0
        self.elksubmitjsons = elksubmitjsons
        # if len(elksubmitjsons) == 0:
        #     raise Exception("Elk submit jsons tasks is need to be useful")

        # data queue for tasks
        self.files_to_read = Queue()
        self.json_to_send = Queue()

        self.keep_running = False
        self.dirchecker_poll_thread = None
        self.jsonreader_poll_thread = None
        self.rmfiles_poll_thread = None
        self.elksubmit_poll_thread = None

    def run_forever(self):
        self.start()
        while True:
            time.sleep(60)

    def start_dircheckers(self):
        self.keep_running = True
        logger.info("Starting the directory checkers")
        for obj in self.dircheckers:
            obj.start()

        t = Thread(target=self.dirchecker_poll)
        self.dirchecker_poll_thread = t
        t.start()
        logger.info("Starting the directory checkers..completed")

    def start_jsonfilereaders(self):
        self.keep_running = True
        logger.info("Starting the file readers")
        for obj in self.jsonfilereaders:
            obj.start()

        t = Thread(target=self.jsonfilereaders_poll)
        self.jsonreader_poll_thread = t
        t.start()
        logger.info("Starting the file readers..completed")

    def start_elksubmitjsons(self):
        logger.info("Starting the elk submitters")
        for obj in self.elksubmitjsons:
            obj.start()

        t = Thread(target=self.elksubmitjson_poll)
        self.elksubmit_poll_thread = t
        t.start()
        logger.info("Starting the json submitters..completed")

    def start_rmfiles(self):
        logger.info("Starting the file rm'ers")
        if self.rmfiles is not None:
            self.rmfiles.start()
            t = Thread(target=self.rmfiles_poll)
            self.rmfiles_poll_thread = t
            t.start()
        logger.info("Starting the file removers..completed")

    def stop_dircheckers(self):
        logger.info("Stopping the dircheckers")
        for obj in self.dircheckers:
            obj.stop()

    def stop_jsonfilereaders(self):
        logger.info("Stopping the json readers")
        for obj in self.jsonfilereaders:
            obj.stop()

    def stop_elksubmitjsons(self):
        logger.info("Stopping the elk submitters")
        for obj in self.elksubmitjsons:
            obj.stop()

    def stop_rmfiles(self):
        logger.info("Stopping the rm'ers")
        if self.rmfiles is not None:
            self.rmfiles.stop()

    def dirchecker_poll(self):
        while self.keep_running:
            data = self.read_dircheckers_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
                continue
            logger.info("Read %d records from dircheckers" % len(data))
            inserted = 0
            for d in data:
                fname = d['filename'] if 'filename' in d else None
                if fname is not None and os.path.isfile(fname):
                    inserted += 1
                    self.add_file_jsonfilereader(d['filename'])

            logger.info("Inserted %d records into jsonfilereaders" % inserted)

    def add_file_jsonfilereader(self, filename):
        if len(self.jsonfilereaders) == 0:
            logger.error("No jsonfilereaders have been defined")
        # jfr = self.jsonfilereaders[self.jfr_pos % self.l_jfr_pos]
        jfr = self.jsonfilereaders[0]
        jfr.add_filename(filename)
        self.jfr_pos += 1

    def jsonfilereaders_poll(self):
        tid = 0
        while self.keep_running:
            data = self.read_jsonfilereaders_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
                continue
            logger.debug("Read %d records from the JSON filereader" % len(data))
            rmed_files = 0
            elkjsonsubmitted = 0
            for d in data:
                json_lines = d['json_lines'] if 'json_lines' in d else None
                filename = d['filename'] if 'filename' in d else None
                status = d['status'] if 'status' in d else None
                tid = d['tid'] if 'tid' in d else tid
                if self.rmfiles is not None and status == 'complete':
                    self.rmfiles.add_filename(tid, filename)
                    rmed_files += 1

                if len(json_lines) > 0:
                    self.add_json_line_elksubmitjson(tid, json_lines)
                    elkjsonsubmitted += len(json_lines)
                    tid += 1

            logger.debug("Inserted %d records into rmfiles queue" % rmed_files)
            logger.debug("Inserted %d records into elkjsonsubmitted queue" % elkjsonsubmitted)

    def rmfiles_poll(self):
        while self.keep_running:
            data = self.read_rmfiles_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
            for d in data:
                tid = d.get('tid', None)
                removed = d.get('removed', 'unknown')
                error = d.get('error', None)
                filename = d.get('filename', 'unknown')
                logger.debug("Remove file completed for %s tid: %s removed: %s error: %s" % (filename, tid, removed, error))

    def add_json_line_elksubmitjson(self, tid, json_lines):
        for esj in self.elksubmitjsons:
            esj.add_json_datas(tid, json_lines)

    def elksubmitjson_poll(self):
        while self.keep_running:
            data = self.read_elksubmitjson_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
                continue
            for d in data:
                tid = d.get('tid', None)
                status = d.get('status', None)
                logger.debug("Elk Submit completed tid: %s status: %s" % (tid, status))

    def start(self):
        self.keep_running = True
        self.start_dircheckers()
        self.start_elksubmitjsons()
        self.start_rmfiles()
        self.start_jsonfilereaders()

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
    def parse_toml_file(cls, toml_file):
        try:
            return cls.parse_toml(toml.load(open(toml_file)))
        except:
            raise Exception("Unable to parse the provided configuration file")

    @classmethod
    def parse_toml(this_cls, toml_dict):

        cs_toml = toml_dict
        if 'json-file-consumer' in toml_dict:
            cs_toml = toml_dict.get('json-file-consumer')

        poll_time = cs_toml.get('poll-time', 20)
        log_level = cs_toml.get('log-level', logging.INFO)
        log_name = cs_toml.get('log-name', this_cls.key())

        dircheckers = []
        jsonfilereaders = []
        rmfiles = None
        elksubmitjsons = []

        cls = TASK_BLOX_MAPPER.get('dirchecker')
        blocks = cs_toml.get('dircheckers', {})
        for block in blocks.values():
            t = block.get('task')
            if t != cls.key():
                continue
            dc = cls.from_toml(block)
            dircheckers.append(dc)

        cls = TASK_BLOX_MAPPER.get('readjsonfile')
        blocks = cs_toml.get('readjsonfiles', {})
        for block in blocks.values():
            t = block.get('task')
            if t != cls.key():
                continue
            dc = cls.from_toml(block)
            jsonfilereaders.append(dc)

        cls = TASK_BLOX_MAPPER.get('elksubmitjson')
        blocks = cs_toml.get('elksubmitjsons', {})
        for block in blocks.values():
            t = block.get('task')
            if t != cls.key():
                continue
            dc = cls.from_toml(block)
            elksubmitjsons.append(dc)

        cls = TASK_BLOX_MAPPER.get('rmfiles')
        block = cs_toml.get('rmfiles', None)
        if block is not None:
            rmfiles = cls.from_toml(block)

        kargs = {
            'dircheckers': dircheckers,
            'jsonfilereaders': jsonfilereaders,
            'rmfiles': rmfiles,
            'elksubmitjsons': elksubmitjsons,
            'poll_time': poll_time,
            'log_level': log_level,
            'log_name': log_name
        }
        return this_cls(**kargs)
