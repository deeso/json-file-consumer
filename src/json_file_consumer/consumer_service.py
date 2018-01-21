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
                 rmfiles=None, jsonupdates=None, elksubmitjsons=[],
                 poll_time=20, log_level=logging.DEBUG, log_name=KEY.lower()):

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
        self.jsonupdates = jsonupdates

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
        self.jsonupdate_poll_thread = None

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

    def start_jsonupdates(self):
        if self.jsonupdates is not None:
            self.keep_running = True
            logger.info("Starting the jsonupdates readers")
            for obj in [self.jsonupdates, ]:
                obj.start()

            t = Thread(target=self.jsonupdate_poll)
            self.jsonupdate_poll_thread = t
            t.start()
            logger.info("Starting the jsonupdates..completed")

    def start_elksubmitjsons(self):
        logger.info("Starting the elk submitters")
        for obj in self.elksubmitjsons:
            obj.start()

        t = Thread(target=self.elksubmitjson_poll)
        self.elksubmit_poll_thread = t
        t.start()
        logger.info("Starting the json submitters..completed")

    def start_rmfiles(self):
        if self.rmfiles is not None:
            logger.info("Starting the file removers")
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

    def stop_jsonupdates(self):
        logger.info("Stopping the elk submitters")
        for obj in [self.jsonupdates, ]:
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
        tid = 0
        found_files = set()
        while self.keep_running:
            json_msgs = self.dc_read_output()
            if len(json_msgs) == 0:
                time.sleep(self.poll_time)
                continue
            logger.info("Read %d records from dircheckers" % len(json_msgs))
            inserted = 0
            for json_msg in json_msgs:
                fname = json_msg.get('filename', None)
                if fname is None or fname in found_files:
                    continue

                found_files.add(fname)
                tid_msg = "%s-%s" % (tid, fname)
                tid += 1
                if 'tid' not in json_msg:
                    json_msg['tid'] = tid_msg

                if os.path.isfile(fname):
                    self.jfr_add_json_msg(json_msg)
                    inserted += 1

            logger.info("Submitted %d records from parsing" % inserted)

    def jfr_add_json_msg(self, json_msg):
        if len(self.jsonfilereaders) == 0:
            logger.error("No jsonfilereaders have been defined")
            raise Exception("Unable to service JSON log file")

        jfr = self.jsonfilereaders[self.jfr_pos % self.l_jfr_pos]
        jfr.add_json_msg(json_msg)
        self.jfr_pos += 1
        return True

    def rmf_add_json_msg(self, json_msg):
        if self.rmfiles is None:
            return False

        fname = json_msg.get('filename', None)
        allowed_to_manip = json_msg.get('allowed_to_manip', False)

        if fname is not None:
            logger.debug("preparing to remove: %s" % fname)
        if allowed_to_manip:
            self.rmfiles.add_json_msg(json_msg)

    def jsonfilereaders_poll(self):
        while self.keep_running:
            json_msgs = self.jfr_read_output()
            m = "jsonfilereaders_poll: Recieved %d messages for processing"
            logger.debug(m % len(json_msgs))
            if len(json_msgs) == 0:
                time.sleep(self.poll_time)
                continue
            for json_msg in json_msgs:
                completed = json_msg.get('completed', False)
                fname = json_msg.get('filename', None)
                if completed:
                    m = "%s is ready to be removed, sending it to rm task"
                    logger.debug(m % fname)
                    self.rmf_add_json_msg(json_msg)

                if self.jsonupdates is not None:
                    self.jsu_add_json_msg(json_msg)
                else:
                    self.esj_add_json_msg(json_msg)

    def jsonupdate_poll(self):

        while self.keep_running:
            json_msgs = self.jsu_read_output()
            m = "jsonupdate_poll: Recieved %d messages for processing"
            logger.debug(m % len(json_msgs))
            if len(json_msgs) == 0:
                time.sleep(self.poll_time)
                continue
            for json_msg in json_msgs:
                self.esj_add_json_msg(json_msg)

    def rmfiles_poll(self):
        lm = "Remove file completed for %s tid: %s removed: %s error: %s"
        while self.keep_running:
            data = self.rmf_read_output()
            if len(data) == 0:
                time.sleep(self.poll_time)
                continue
            for d in data:
                tid = d.get('tid', None)
                removed = d.get('removed', 'unknown')
                error = d.get('error', None)
                filename = d.get('filename', 'unknown')
                logger.debug(lm % (filename, tid, removed, error))

    def esj_add_json_msg(self, json_msg):
        for esj in self.elksubmitjsons:
            esj.add_json_msg(json_msg)

    def jsu_add_json_msg(self, json_msg):
        for jsu in [self.jsonupdates, ]:
            if jsu is not None:
                jsu.add_json_msg(json_msg)

    def elksubmitjson_poll(self):
        while self.keep_running:
            data = self.esj_read_output()
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

    def generic_read_queue(self, objs):
        out_data = []
        while len(out_data) < 1000:
            empty = 0
            for obj in objs:
                json_msg = obj.read_outqueue()
                if json_msg is None:
                    empty += 1
                    continue
                out_data.append(json_msg)

            if empty == len(objs):
                break
        return out_data

    def jfr_read_output(self):
        return self.generic_read_queue(self.jsonfilereaders)

    def dc_read_output(self):
        return self.generic_read_queue(self.dircheckers)

    def rmf_read_output(self):
        return self.generic_read_queue([self.rmfiles, ])

    def esj_read_output(self):
        return self.generic_read_queue(self.elksubmitjsons)

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
        jsonupdates = None

        blocks = cs_toml.get('dircheckers', {})
        for block in blocks.values():
            t = block.get('task')
            cls = TASK_BLOX_MAPPER.get(t, None)
            if cls is None:
                continue
            dc = cls.parse_toml(block)
            dircheckers.append(dc)

        blocks = cs_toml.get('readjsonfiles', {})
        for block in blocks.values():
            t = block.get('task')
            cls = TASK_BLOX_MAPPER.get(t, None)
            if cls is None:
                continue
            dc = cls.parse_toml(block)
            jsonfilereaders.append(dc)

        blocks = cs_toml.get('elksubmitjsons', {})
        for block in blocks.values():
            t = block.get('task')
            cls = TASK_BLOX_MAPPER.get(t, None)
            if cls is None:
                continue
            dc = cls.parse_toml(block)
            elksubmitjsons.append(dc)

        blocks = cs_toml.get('jsonupdates', None)
        if blocks is not None:
            t = blocks.get('task')
            cls = TASK_BLOX_MAPPER.get(t, None)
            if cls is None:
                continue
            jsonupdates = cls.parse_toml(blocks)

        blocks = cs_toml.get('rmfiles', None)
        if blocks is not None:
            t = blocks.get('task')
            cls = TASK_BLOX_MAPPER.get(t, None)
            if cls is None:
                continue
            rmfiles = cls.parse_toml(blocks)
            # logger.debug("rmfiles block = %s" % str(block))
            # logger.debug("rmfiles value = %s" % str(rmfiles))

        kargs = {
            'dircheckers': dircheckers,
            'jsonfilereaders': jsonfilereaders,
            'rmfiles': rmfiles,
            'elksubmitjsons': elksubmitjsons,
            'jsonupdates': jsonupdates,
            'poll_time': poll_time,
            'log_level': log_level,
            'log_name': log_name
        }
        return this_cls(**kargs)
