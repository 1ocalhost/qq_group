import os
import json


class Statistics:
    def __init__(self):
        self.data = []

    def run(self):
        for n in range(0, 143):
            prefix = str(n).zfill(3)
            path = prefix[0:2] + '/' + prefix[2:]
            self.on_path(path)

        self.show()

    def show(self):
        print(json.dumps(self.data))


class UserNumber(Statistics):
    def get_user_num(self, path):
        num = 0

        def counter(value):
            nonlocal num
            id = int.from_bytes(value, byteorder='big')
            if id != ((1 << 32) - 1):
                num += 1

        with open(path, 'rb') as file:
            while True:
                value = file.read(4)
                if not value:
                    break
                counter(value)

        return num

    def on_path(self, path):
        num = 0
        try:
            file_path = path + '.map'
            print(file_path)
            num = self.get_user_num(file_path)
        except FileNotFoundError:
            pass

        self.data.append(num)


class GroupNumber(Statistics):
    def on_path(self, path):
        size = 0
        try:
            size = os.path.getsize(path + '.data')
        except FileNotFoundError:
            pass

        self.data.append(int(size / 4))


def main():
    GroupNumber().run()
    UserNumber().run()


if __name__ == '__main__':
    main()
