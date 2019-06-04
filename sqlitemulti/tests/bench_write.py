"""
Benchmark: direct db write from several threads vs queued
"""

import os
import sys
import random
import sqlite3
import threading
from time import time

sys.path.append("../")
from sqlitemulti.sqlitemulti import SqliteMulti


THREAD_COUNT = 10
RUN_COUNT = 10

# Create test table
SQL_CREATE = "CREATE TABLE IF NOT EXISTS transactions (" \
             "timestamp TEXT, address TEXT, amount TEXT)"

SQL_INSERT = "INSERT INTO transactions VALUES (?,?,?)"

DATA = []

LOCK = threading.Lock()


def make_data():
    global DATA
    random.seed("let do this deterministic")
    for runs in range(RUN_COUNT):
        run = []
        for lines in range(THREAD_COUNT):
            # Fake data
            line = (random.randint(0, 999999), random.randint(0, 9999999999), random.randint(0, 99999999999999)/1e8)
            run.append(line)
        DATA.append(run)


def sqlite_writer(t_index, db, sql, params):
    with LOCK:
        db.execute(sql, params)
        db.commit()


def bench_direct():
    # write from threads with lock - 17sec
    if os.path.isfile("bench1.db"):
        os.remove("bench1.db")
    db = sqlite3.connect("bench1.db", check_same_thread=False)
    db.execute(SQL_CREATE)
    db.commit()
    for i in range(RUN_COUNT):
        threads = []
        for t in range(THREAD_COUNT):
            thread = threading.Thread(
                target=sqlite_writer,
                args=(t, db, SQL_INSERT, DATA[i][t]),
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()


def bench_queue():
    # write from SqliteMulti
    if os.path.isfile("bench2.db"):
        os.remove("bench2.db")
    db = SqliteMulti.connect("bench2.db", verbose=False)
    db.execute(SQL_CREATE, commit=True)  # Will do sql + commit
    for i in range(RUN_COUNT):
        for t in range(THREAD_COUNT):
            db.execute(SQL_INSERT, DATA[i][t], commit=True)
    db.stop()
    # Wait for end
    db.join()


if __name__ == "__main__":

    make_data()
    # print(DATA)
    start = time()
    bench_direct()
    total = time() - start
    print(f"Direct: {total} s")

    start = time()
    bench_queue()
    total = time() - start
    print(f"Queue: {total} s")

