"""
Author: Oleg Shkolnik יב9.
Description: there are three classes for database in this program:
                first creates dictionary and has basic operations with it: set, get and delete value;
                
                second works with pickle, saving data in pkl file: loads and dumps data to/from this file;
                
                third receives instruction if it needs too work with treads or processes and then
                creates semaphores and locks that allow working for limited number of users:
                10 readers at the same time, one writer at the same time without any readers.
Date: 15/11/24
"""

import pickle
import multiprocessing
import threading


# default file for database
filename = 'database.pkl'
get_fail = "This key is not in the database"


class BasicDataBase:
    def __init__(self):
        # creating dictionary from/in which will be loaded data in/from the file
        self.base = dict()

    def set_value(self, key, value):
        """
        checks if key isn't used, sets key with the value if it isn't
        :param key: key for database
        :param value: value for database
        :return: true/false if key is used in database
        """
        res = True
        if key in self.base:
            res = False
        else:
            self.base[key] = value
        return res

    def get_value(self, key):
        # looks for key in database, returns data that locates with the key or fail message
        result = get_fail
        if key in self.base:
            result = self.base[key]
        return result

    def delete_value(self, key):
        # checks if key is in the database and delete the data if it is
        if key in self.base:
            val = self.base[key]
            del self.base[key]
            return val


class PickleBase(BasicDataBase):
    global filename

    def __init__(self, data=filename):
        super().__init__()
        self.data = data

    def set_value(self, key, value):
        # function inherits 'basic database' function and also load result into pkl file
        while not self.load_from_file():
            continue
        res = super().set_value(key, value)
        while not self.save_to_file():
            continue
        return res

    def delete_value(self, key):
        # function inherits 'basic database' function and also load result into pkl file
        self.load_from_file()
        super().delete_value(key)
        while not self.save_to_file():
            continue

    def get_value(self, key):
        # function loads data from pkl file and then inherits 'basic database'
        while not self.load_from_file():
            continue
        return super().get_value(key)

    def save_to_file(self):
        # saving data into the file
        res = True
        try:
            with open(self.data, 'wb') as f:
                pickle.dump(self.base, f)
        except Exception as err:
            print(f'program got {err}')
            res = False
        finally:
            return res

    def load_from_file(self):
        # loading data from the file
        res = True
        try:
            with open(self.data, 'rb') as f:
                self.base = pickle.load(f)
        except Exception as err:
            print(f'program got {err}')
            res = False
        finally:
            return res


class DataBase(PickleBase):

    def __init__(self, flag=True, db=filename):
        """
        function inherits 'picklebase', checks if client wants to work with threads or processes,
        according to this creates semaphores and locks
        :param flag: choosing if client want to work with threads or processes
        :param db: filename of database
        """
        super().__init__(db)
        self.n = 10
        if flag:
            self.readers_semaphore = multiprocessing.Semaphore(self.n)
            self.readers_lock = multiprocessing.Lock()
            self.writer_lock = multiprocessing.Lock()
            self.data_lock = multiprocessing.Lock()
        else:
            self.readers_semaphore = threading.Semaphore(self.n)
            self.readers_lock = threading.Lock()
            self.writer_lock = threading.Lock()
            self.data_lock = multiprocessing.Lock()

    def set_value(self, key, value):
        """
        function starts working with locks,
        takes all the places in semaphore (for readers) and puts the data using inherited function,
        at the end it releases semaphore.
        :return: true/false if it got data
        """
        with self.writer_lock:
            with self.readers_lock:
                for i in range(self.n):
                    self.readers_semaphore.acquire()
                with self.data_lock:
                    res = super().set_value(key, value)
                for j in range(self.n):
                    self.readers_semaphore.release()
        return res

    def delete_value(self, key):
        """
        function starts working with locks,
        takes all the places in semaphore (for readers) and changes the data using inherited function,
        at the end it releases semaphore
        """
        with self.writer_lock:
            with self.readers_lock:
                for i in range(self.n):
                    self.readers_semaphore.acquire()
            with self.data_lock:
                super().delete_value(key)
            for j in range(self.n):
                self.readers_semaphore.release()

    def get_value(self, key):
        """
        function checks if there is place to read the data from database for 1 second,
        take 1 place in semaphore, inherits get function and release this place
        :return: data that it got or null
        """
        data = ''
        acquired = self.readers_semaphore.acquire(timeout=1)
        if acquired:
            try:
                with self.data_lock:
                    data = super().get_value(key)
            finally:
                self.readers_semaphore.release()
                return data
        else:
            print("there are too much readers")


if __name__ == '__main__':
    login = 1
    password = 1

    test_db1 = BasicDataBase()
    assert test_db1.set_value(login, password) == True
    assert test_db1.get_value(login) == password
    test_db1.delete_value(login)
    assert test_db1.get_value(login) != password

    test_db2 = PickleBase()
    assert test_db2.set_value(login, password) == True
    assert test_db2.get_value(login) == password
    test_db2.delete_value(login)
    assert test_db2.get_value(login) != password

    test_db3 = DataBase()
    assert test_db3.set_value(login, password) == True
    assert test_db3.get_value(login) == password
    test_db3.delete_value(login)
    assert test_db3.get_value(login) != password

    test_db4 = DataBase(False)
    assert test_db4.set_value(login, password) == True
    assert test_db4.get_value(login) == password
    test_db4.delete_value(login)
    assert test_db4.get_value(login) != password
