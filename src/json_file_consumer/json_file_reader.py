from .task_dirchecker import DirCheckerTask
from .task_elksumbmitjson import ElkSubmitJsonTask
from .task_readjsonfile import ReadJsonFileTask
from .task_rmfiles import ReadJsonFileTask


EVE_PATTERN = '.*eve-\d{4}-\d{2}-\d{2}-\d{2}:\d{2}\.json'

class JsonFileBeat(object):

    def __init__(self, name, log_level=logging.DEBUG,
                 poll_time=60, target_directory=None,
                 remove_after_read=False, base_name=None,
                 subscribers=[], e_key=None,
                 compress=False, salt=None, encrypt=False,
                 name_pattern=EVE_PATTERN, content_filters=[], num_procs=10):
        self.name = name
        self.encrypt = False if encrypt is None or e_key is None else True
        self.log_level = log_level
        self.poll_time = 60
        self.target_directory = target_directory
        self.remove_after_read = False
        self.subscribers = subscribers
        self.name_pattern = name_pattern
        self.content_filters = content_filters
        self.files_read = []

        self.last_files_observed = []
        self.read_files = []
        self.continue_polling = False
        self.in_queue = Queue()
        self.out_queue = Queue()
        self.file_read_process = None
        self.service = None
        self.poll_queue = False
        self.poll_thread = None
        self.result_queue = []
        self.result_lock = thread.Lock()

        self.read_filenames = set()
        self.process_files = False

        self.consume_thread = None

    def handle_results(self, results):
        jdata = []
        for r in results:
            filename = r['name']
            if filename in self.read_filenames:
                continue
            self.read_filenames.add(filename)
            jdata = [json.loads(i) for i in r['data']]

        self.result_lock.acquire()
        self.result_data = self.result_data + jdata
        self.result_lock.release()

    def get_result_data(self):
        r = []
        self.result_lock.acquire()
        r = self.result_data
        self.result_data = []
        self.result_lock.release()
        return r

    def shutdown(self):
        self.continue_polling = False
        self.file_read_process.terminate()
        self.file_read_process.join()
        self.poll_thread.join()
        self.consume_thread.join()

    def start(self):
        self.file_read_process = Process(target=DirCheckReaderWorker.do_work,
                                         args=(self.in_queue, self.out_queue,
                                               self.name_pattern,
                                               self.target_directory))
        self.file_read_process.start()
        self.poll_thread = thread.Threading(target=(self.poll_queue_forever))
        self.poll_thread.start()
        self.consume_thread = thread.Threading(target=(self.consume_forever))
        self.consume_thread.start()

    def consume_forever(self):
        while True:
            if len(self.result_data):
                self.consumue_and_publish()
            if not self.continue_polling:
                break
            time.sleep(self.poll_time)
            if not self.continue_polling:
                break

    def poll_queue_forever(self):
        while True:
            if not self.out_queue.empty():
                while not self.out_queue.empty():
                    r = self.out_queue.get()
                    self.handle_results(r)
                    if not self.continue_polling:
                        break

            if not self.continue_polling:
                break

            time.sleep(self.poll_time)

    def consume_and_publish(self):
        json_data = self.get_result_data()
        self.publish(json_data)

    # def check_directory(self):
    #     files_to_read = []
    #     files = os.listdir(self.target_directory)
    #     files = [os.path.join(self.target_directory, i) for i in files]
    #     observed = [i for i in files if i not in self.read_files]
    #     files_to_read = [o for o in observed if not self.is_opened(o)]
    #     return files_to_read

    # def read_files(self, files_to_read):
    #     json_data = []
    #     for name in files_to_read:
    #         if name in self.read_files:
    #             continue
    #         self.read_files.append(name)
    #         lines = open(name).readlines()
    #         for l in lines:
    #             j = json.loads(l.strip())
    #             json_data.append(j)
    #     return json_data

    def publish(self, json_data):
        for s in self.subscribers:
            self.send_to_subscriber(s, json_data)

    def send_to_subscriber(self, s, json_data):
        for j in json_data:
            s.publish(j)

    @classmethod
    def parse_toml_dict(cls, toml_dict):
        
