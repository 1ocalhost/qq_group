import os
import mmap
import time
import multiprocessing as mp
from base import interval_call


def comma(x):
    return '{:,}'.format(x)


class DataFileReader:
    def __init__(self, db_name, filter):
        self.db_name = db_name
        self.filter = filter
        self.file = open(f'dump_{db_name}.data', 'rb')
        self.read_num = 0

    @staticmethod
    def bytes_to_int(x):
        return int.from_bytes(x, byteorder='big')

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            user, group = self.read_next()
            if user in self.filter:
                return (user, group)

    def read_next(self):
        DATA_SIZE = 8
        data = self.file.read(DATA_SIZE)
        if not data:
            raise StopIteration

        assert(len(data) == DATA_SIZE)
        user = self.bytes_to_int(data[0:4])
        group = self.bytes_to_int(data[4:8])
        self.read_num += 1
        return (user, group)

    def status(self):
        return f'{self.db_name}@ {comma(self.read_num)}'


class DataBaseReaderImpl:
    def __init__(self, begin, end):
        self.filter = range(begin, end)
        self.db_begin = Config().DB_BEGIN
        self.db_end = Config().DB_END

    def next(self):
        if self.db_begin > self.db_end:
            return None

        reader = DataFileReader(self.db_begin, self.filter)
        self.db_begin += 1
        return reader


class DataBaseReader:
    def reset_task(self, begin, end):
        self.impl = DataBaseReaderImpl(begin, end)

    def next(self):
        return self.impl.next()


class DataIndex:
    def __init__(self):
        self.MAX_ID = (1 << 32)
        self.ID_WIDTH = len(str(self.MAX_ID))
        scan_max = Config().TASK_SPACE - 1
        self.SCAN_WITH = len(str(scan_max))
        self.init()

    def init(self):
        self.record_num = 0

    def to_id_str(self, id):
        return str(id).zfill(self.ID_WIDTH)

    # range is [begin, end)
    def scan_user_range(self, reader, begin):
        self.begin = begin
        self.init()
        self.bucket = {}
        end = begin + Config().TASK_SPACE
        reader.reset_task(begin, end)

        while True:
            data = reader.next()
            if data is None:
                return
            self.cur_data = data
            self.dispatch_data(data)

    def dispatch_data(self, data):
        for row in data:
            user, group = row
            self.insert_record(user, group)

    @interval_call
    def log(self):
        print('Records:', self.record_num,
              '  begin:', comma(self.begin),
              '  FileReader:', self.cur_data.status())

    def insert_record(self, user, group):
        assert(user < self.MAX_ID)
        assert(group < self.MAX_ID)

        self.bucket.setdefault(user, []).append(group)
        self.record_num += 1
        self.log()

    def create_dumper_file(self):
        task_with = self.ID_WIDTH - self.SCAN_WITH
        task_leading = self.to_id_str(self.begin)[0:task_with]
        path = task_leading[:2] + '/' + task_leading[2:]
        return FileWriter(path, self.SCAN_WITH)

    def dump_to_file(self):
        file = self.create_dumper_file()
        for user, group in sorted(self.bucket.items()):
            file.map_add_record(user)
            for group_item in group:
                file.data_add_record(group_item)
        file.close()


class FileWriter:
    def __init__(self, path, space_with):
        assert space_with <= 7
        self.space_range = 10 ** space_with
        self.map_offset = 0
        path = 'index/' + path
        self.create_map(path + '.map')
        self.data_file = open(path + '.data', 'wb')

    def data_size(self):
        return self.map_offset

    @staticmethod
    def int_to_bytes(x):
        BIT_NUM = 4
        MASK = (1 << (BIT_NUM * 8))
        return (x % MASK).to_bytes(BIT_NUM, 'big')

    @staticmethod
    def ensure_open(filename, flag):
        dirpath = os.path.dirname(filename)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        return open(filename, flag)

    def create_map(self, path):
        with self.ensure_open(path, 'wb') as f:
            f.write(self.space_range * self.int_to_bytes(-1))
            f.close()

        self.map_file = open(path, 'r+b')
        self.mm = mmap.mmap(self.map_file.fileno(), 0)

    def map_add_record(self, user):
        scoped_user = user % self.space_range
        start = scoped_user * 4
        content = self.int_to_bytes(self.map_offset)
        self.mm[start: start + 4] = content

    def data_add_record(self, group):
        content = self.int_to_bytes(group)
        self.data_file.write(content)
        self.map_offset += 4

    def close(self):
        self.mm.close()
        self.map_file.close()
        self.data_file.close()


class Config:
    TASK_CEIL = (1 << 32)
    TASK_SPACE = 10 ** 7
    DB_BEGIN = 1
    DB_END = 11


def worker(begin):
    index = DataIndex()
    reader = DataBaseReader()
    index.scan_user_range(reader, begin)
    index.dump_to_file()


def main():
    c = Config()
    with mp.Pool(20) as p:
        tasks = range(c.TASK_SPACE * 0, c.TASK_CEIL, c.TASK_SPACE)
        p.map(worker, [t for t in tasks], chunksize=1)


if __name__ == '__main__':
    main()
