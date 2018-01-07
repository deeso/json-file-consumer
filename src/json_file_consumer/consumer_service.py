from task_blox import TASK_BLOX_MAPPER


EVE_PATTERN = '.*eve-\d{4}-\d{2}-\d{2}-\d{2}:\d{2}\.json'

class ConsumerService(object):

    def __init__(self, dircheckers=[], jsonfilereaders=[], rmfiles=None, elksubmitjsons=[]):
        self.dircheckers = dircheckers

        if len(dircheckers) == 0:
            raise Exception("Directory checking tasks is need to be useful")

        self.jsonfilereaders = jsonfilereaders
        if len(dircheckers) == 0:
            raise Exception("JSON File readers tasks is need to be useful")

        self.rmfiles = rmfiles

        self.elksubmitjsons = elksubmitjsons
        if len(elksubmitjsons) == 0:
            raise Exception("Elk submit jsons tasks is need to be useful")

    @classmethod
    def parse_toml(cls, toml_dict):

        cs_toml = toml_dict
        if 'json-file-consumer' in toml_dict:
            cs_toml = toml_dict.get('json-file-consumer')

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
        }

        return cls(**kargs)
