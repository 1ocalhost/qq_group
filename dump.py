import math
import subprocess
import _mssql
from base import interval_call


class DataBaseReader:
    # db_num in [1, 11]
    def __init__(self):
        self.table_id = 1
        self.table_id_max = 0
        self.conn = _mssql.connect(server='192.168.244.128',
                                   user='demo', password='demo')

    def reset_db(self, db_num):
        self.table_id_max = db_num * 100
        self.table_id = self.table_id_max - 99

    def close(self):
        self.conn.close()

    def next(self):
        if self.table_id > self.table_id_max:
            return None

        self.query_next()
        self.table_id += 1
        return self.conn

    def query_next(self):
        database_id = math.floor((self.table_id - 1) / 100) + 1
        table_name = f'[GroupData{database_id}_Data]' + \
                     f'.[dbo].[Group{self.table_id}]'
        sql = f'SELECT QQNum, QunNum FROM {table_name}'
        print(sql)
        self.conn.execute_query(sql)


class FileDumper:
    def __init__(self):
        self.record_num = 0

    @staticmethod
    def int_to_bytes(x):
        return x.to_bytes(4, 'big', signed=True)

    def create(self, path):
        self.file = open(path, 'wb')

    def close(self):
        print('record_num:', self.record_num, '[close]')
        self.file.close()

    @interval_call
    def log(self):
        print(f'record_num: {self.record_num}')

    def dump(self, reader):
        while True:
            data = reader.next()
            if data is None:
                return
            self.append_rows(data)

    def append_rows(self, data):
        for row in data:
            user = row['QQNum']
            group = row['QunNum']
            self.append_item(user, group)

    def append_item(self, user, group):
        self.record_num += 1
        self.file.write(self.int_to_bytes(user))
        self.file.write(self.int_to_bytes(group))
        self.log()


def dump_to_file(db, reader):
    dumper = FileDumper()
    dumper.create(f'dump_{db}.data')
    dumper.dump(reader)
    dumper.close()


def main():
    reader = DataBaseReader()
    for db in range(1, 12):
        reader.reset_db(db)
        dump_to_file(db, reader)


if __name__ == '__main__':
    subprocess.call('cls', shell=True)
    main()
