from database import *


fail_reading = "failed to read data, key was lost or access is blocked"
#creating database
flag = False
db = DataBase(flag)
db.set_value(1, 1)
db.delete_value(1)


def set_value(key, value):
    # Uses class database to set value. Receives answer and checks if data was set. Print answer
    res = db.set_value(key, value)
    if res:
        print('data was written')
    else:
        print("data wasn't written")


def delete_value(key):
    # Uses class database to delete value
    db.delete_value(key)


def get_value(key):
    # Uses class database to get value using the key. Function checks if the value isn't null and prints it
    value = db.get_value(key)
    if value is None:
        print("failed to read data, key was lost or access is blocked")
    else:
        print(value)


def first_test():
    # first test that checks if functions are working
    login1 = 1
    password1 = 2

    set_value(login1, password1)
    get_value(1)


# second and third tests create 5 reading process and writer process.
# they check if database works right when writer or reader starts first
def second_test():
    login2 = 2
    password2 = 3

    key2 = 1

    writer_thread = threading.Thread(target=set_value, args=(login2, password2))
    reader_threads = [threading.Thread(target=get_value, args=(key2,)) for _ in range(5)]

    for r_thread in reader_threads:
        r_thread.start()

    writer_thread.start()

    for r_thread in reader_threads:
        r_thread.join()
    writer_thread.join()


def third_test():

    login3 = 3
    password3 = 4

    writer_thread = threading.Thread(target=set_value, args=(login3, password3))
    reader_threads = [threading.Thread(target=get_value, args=(login3,)) for _ in range(10)]

    writer_thread.start()
    for r_thread in reader_threads:
        r_thread.start()

    writer_thread.join()
    for r_thread in reader_threads:
        r_thread.join()


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

    writer_thread1 = [threading.Thread(target=set_value, args=(login1, password1)) for _ in range(33)]
    reader_threads1 = [threading.Thread(target=get_value, args=(login1,)) for _ in range(33)]
    writer_thread2 = [threading.Thread(target=set_value, args=(login2, password2)) for _ in range(33)]
    reader_threads2 = [threading.Thread(target=get_value, args=(login2,)) for _ in range(33)]
    writer_thread3 = [threading.Thread(target=set_value, args=(login3, password3)) for _ in range(33)]
    reader_threads3 = [threading.Thread(target=get_value, args=(login3,)) for _ in range(33)]

    for w_thread in writer_thread1:
        w_thread.start()
    for r_thread in reader_threads1:
        r_thread.start()
    for w_thread in writer_thread2:
        w_thread.start()
    for r_thread in reader_threads2:
        r_thread.start()
    for w_thread in writer_thread3:
        w_thread.start()
    for r_thread in reader_threads3:
        r_thread.start()

    for w_thread in writer_thread1:
        w_thread.join()
    for r_thread in reader_threads1:
        r_thread.join()
    for w_thread in writer_thread2:
        w_thread.join()
    for r_thread in reader_threads2:
        r_thread.join()
    for w_thread in writer_thread3:
        w_thread.join()
    for r_thread in reader_threads3:
        r_thread.join()

    get_value(login1)
    get_value(login2)
    get_value(login3)


if __name__ == '__main__':
    final_test()
    db.delete_value(10)
    db.delete_value(20)
    db.delete_value(30)
