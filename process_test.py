from database import *
import multiprocessing


fail_reading = "failed to read data, key was lost or access is blocked"
#creating database
flag = True
db = DataBase(flag)


def set_value(key, value):
    # Uses class database to set value. Receives answer and checks if data was set. Print answer
    res = db.set_value(key, value)
    if res:
        print("data was written")
    else:
        print("data wasn't written")


def delete_value(key):
    # Uses class database to delete value
    db.delete_value(key)


def get_value(key):
    # Uses class database to get value using the key. Function checks if the value isn't null and prints it
    value = db.get_value(key)
    if value is None:
        print(fail_reading)
    else:
        print(value)


def first_test():
    # first test that checks if functions are working
    login1 = 1
    password1 = 2

    set_value(login1, password1)
    get_value(login1)


# second and third tests create 5 reading process and writer process and check if database works right
def second_test():
    login2 = 2
    password2 = 3
    key2 = 1

    writer_process = multiprocessing.Process(target=set_value, args=(login2, password2))
    reader_processes = [multiprocessing.Process(target=get_value, args=(key2,)) for _ in range(5)]

    for r_process in reader_processes:
        r_process.start()
    writer_process.start()

    for r_process in reader_processes:
        r_process.join()
    writer_process.join()


def third_test():
    login3 = 3
    password3 = 4
    key3 = 3

    writer_process = multiprocessing.Process(target=set_value, args=(login3, password3))
    reader_processes = [multiprocessing.Process(target=get_value, args=(key3,)) for _ in range(5)]

    writer_process.start()
    for r_process in reader_processes:
        r_process.start()

    writer_process.join()
    for r_process in reader_processes:
        r_process.join()


def final_test():
    """
    final test creates 99 reading processes and writing processes
    all these processes are divided into three groups
    test starts all processes and checks if in the end data will be saved right
    """
    login1 = 10
    password1 = 10
    login2 = 20
    password2 = 20
    login3 = 30
    password3 = 30

    writer_process1 = [multiprocessing.Process(target=set_value, args=(login1, password1)) for _ in range(33)]
    writer_process2 = [multiprocessing.Process(target=set_value, args=(login2, password2)) for _ in range(33)]
    writer_process3 = [multiprocessing.Process(target=set_value, args=(login3, password3)) for _ in range(33)]
    reader_processes1 = [multiprocessing.Process(target=get_value, args=(login1,)) for _ in range(33)]
    reader_processes2 = [multiprocessing.Process(target=get_value, args=(login2,)) for _ in range(33)]
    reader_processes3 = [multiprocessing.Process(target=get_value, args=(login3,)) for _ in range(33)]

    for w_process in writer_process1:
        w_process.start()
    for r_process in reader_processes1:
        r_process.start()

    for w_process in writer_process2:
        w_process.start()
    for r_process in reader_processes2:
        r_process.start()

    for w_process in writer_process3:
        w_process.start()
    for r_process in reader_processes3:
        r_process.start()

    for w_process in writer_process1:
        w_process.join()
    for r_process in reader_processes1:
        r_process.join()


    for w_process in writer_process2:
        w_process.join()
    for r_process in reader_processes2:
        r_process.join()

    for w_process in writer_process3:
        w_process.join()
    for r_process in reader_processes3:
        r_process.join()

    get_value(login1)
    get_value(login2)
    get_value(login3)


if __name__ == '__main__':
    final_test()
