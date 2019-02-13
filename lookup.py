from abc import ABCMeta, abstractmethod
import json
import requests


class IFileReader(metaclass=ABCMeta):
    @abstractmethod
    def read_impl(self, path, pos, size):
        pass

    def read(self, path, pos, size):
        data = self.read_impl(path, pos, size)
        if not data:
            return []

        f = self.byte_to_int
        r = range(0, len(data), 4)
        return [f(data[n:n + 4]) for n in r]

    @staticmethod
    def byte_to_int(b):
        return int.from_bytes(b, byteorder='big', signed=True)


class LocalFileReader(IFileReader):
    def read_impl(self, path, pos, size):
        with open(path, 'rb') as f:
            f.seek(pos)
            return f.read(size)


class OneDriveReader(IFileReader):
    def __init__(self, cookie=None):
        self.load_config()

        if cookie is None:
            self.init_access()
        else:
            self.cookie = cookie

    def load_config(self):
        with open('auth.json') as f:
            data = json.load(f)
            config = data['one_drive']

        self.SHARED_LINK = config['shared_link']
        self.DOWNLOAD_LINK = config['download_link']
        self.DOWNLOAD_DIR_BASE = config['download_dir_base']

    def init_access(self):
        url = self.SHARED_LINK
        cookie = None
        for i in range(0, 5):
            print('GET', i, url)
            r = requests.get(url, allow_redirects=False)
            if 'set-cookie' in r.headers:
                cookie = r.headers['set-cookie'].split(';')[0]
                break

            if 'location' in r.headers:
                url = r.headers['location']
                continue

        assert(cookie is not None)
        self.cookie = cookie

    def read_impl(self, path, pos, size):
        end_pos = pos + size - 1
        headers = {
            'cookie': self.cookie,
            'range': f'bytes={pos}-{end_pos}'
        }

        args = {'SourceUrl': self.DOWNLOAD_DIR_BASE + path}
        r = requests.get(self.DOWNLOAD_LINK, args, headers=headers)
        assert(r.status_code in [206])
        return r.content


class GroupData:
    def __init__(self, *args, **kwargs):
        self.file = OneDriveReader(*args, **kwargs)

    def has_info_with_id(self, full_id):
        prefix = int(full_id[0:3])

        if prefix in range(0, 133):
            return True

        if prefix in range(140, 143):
            return True

        return False

    def find_data_boundary_impl(self, path, user_id, max_number):
        INT_WIDTH = 4
        NO_RECORD = 0, 0

        offset = (user_id % (10 ** 7)) * INT_WIDTH
        length = max_number * INT_WIDTH
        view = self.file.read(path + '.map', offset, length)
        assert(len(view) != 0)

        record_pos = view[0]
        if record_pos == -1:
            return NO_RECORD

        next_pos = -1
        for i in view[1:]:
            if i != -1:
                next_pos = i
                break

        return record_pos, next_pos

    def find_data_boundary(self, path, user_id):
        for max_number in [20, 200, 2000]:
            record_pos, next_pos = self.find_data_boundary_impl(
                path, user_id, max_number)
            if next_pos != -1:
                break

        assert(record_pos >= 0)
        assert(next_pos != -1)
        return record_pos, next_pos

    def find_group_list(self, path, record_pos, next_pos):
        record_size = next_pos - record_pos
        assert(record_size in range(0, 1000))

        if record_size == 0:
            return []

        return self.file.read(path + '.data', record_pos, record_size)

    def lookup(self, user_id):
        full_id = str(user_id).zfill(10)
        path = full_id[:2] + '/' + full_id[2:3]

        if not self.has_info_with_id(full_id):
            return []

        record_pos, next_pos = self.find_data_boundary(path, user_id)
        return self.find_group_list(path, record_pos, next_pos)


def main():
    data = GroupData()
    print(data.lookup(10001))


if __name__ == '__main__':
    main()
