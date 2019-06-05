"""
Benchmark: direct db write from several threads vs queued
"""

import os
import sys
import random
import sqlite3
import threading
from time import time, sleep

sys.path.append("../")
from sqlitemulti.sqlitemulti import SqliteMulti


THREAD_COUNT = 10
RUN_COUNT = 10

# Create test table
SQL_CREATE = (
    "CREATE TABLE IF NOT EXISTS transactions ("
    "timestamp TEXT, address TEXT, amount TEXT)"
)

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
            line = (
                random.randint(0, 999999),
                random.randint(0, 9999999999),
                random.randint(0, 99999999999999) / 1e8,
            )
            run.append(line)
        DATA.append(run)


def bench_direct_single(single_commits=True):
    # write from the main thread in sequence
    if os.path.isfile("bench0.db"):
        os.remove("bench0.db")
    db = sqlite3.connect("bench0.db")
    db.execute(SQL_CREATE)
    db.commit()
    for i in range(RUN_COUNT):
        for t in range(THREAD_COUNT):
            db.execute(SQL_INSERT, DATA[i][t])
            if single_commits:
                db.commit()
    if not single_commits:
        db.commit()


def bench_direct_many():
    # write from the main thread in sequence, by executemany batches
    if os.path.isfile("bench4.db"):
        os.remove("bench4.db")
    db = sqlite3.connect("bench4.db")
    db.execute(SQL_CREATE)
    db.commit()
    for i in range(RUN_COUNT):
        db.executemany(SQL_INSERT, DATA[i])
        db.commit()


def sqlite_writer(t_index, db, sql, params, commit):
    with LOCK:
        db.execute(sql, params)
        if commit:
            db.commit()


def bench_direct(single_commits=True):
    # write from threads, single db handler whith check_same_thread=False, write Lock
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
                args=(t, db, SQL_INSERT, DATA[i][t], single_commits),
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()
    if not single_commits:
        db.commit()


def bench_queue(single_commits=True):
    # write from SqliteMulti
    if os.path.isfile("bench2.db"):
        os.remove("bench2.db")
    db = SqliteMulti.connect("bench2.db", isolation_level="", verbose=False)
    db.execute(SQL_CREATE, commit=True)  # Will do sql + commit
    for i in range(RUN_COUNT):
        for t in range(THREAD_COUNT):
            if single_commits:
                db.execute(SQL_INSERT, DATA[i][t], commit=True)
            else:
                db.execute(SQL_INSERT, DATA[i][t], commit=False)
    if not single_commits:
        db.commit()
    db.stop()
    #  Wait for end
    db.join()


def bench_queue_transaction():
    # write from SqliteMulti, by sending him batch of sql to execute as transactions with implicit commit at the end.
    if os.path.isfile("bench3.db"):
        os.remove("bench3.db")
    db = SqliteMulti.connect("bench3.db", isolation_level="", verbose=False)
    db.execute(SQL_CREATE, commit=True)  # Will do sql + commit
    sql = [SQL_INSERT for i in range(THREAD_COUNT)]
    for i in range(RUN_COUNT):
        # sql and params both are lists
        db.execute(sql, DATA[i], commit=True)
    db.stop()
    db.join()


def bench_queue_many():
    # write from SqliteMulti, by sending him batch of sql to execute as transactions with implicit commit at the end.
    if os.path.isfile("bench5.db"):
        os.remove("bench5.db")
    db = SqliteMulti.connect("bench5.db", isolation_level="", verbose=False)
    db.execute(SQL_CREATE, commit=True)  # Will do sql + commit
    for i in range(RUN_COUNT):
        # sql is str, params a lists
        db.executemany(SQL_INSERT, DATA[i])
    db.stop()
    db.join()


if __name__ == "__main__":

    make_data()
    # print(DATA)

    start = time()
    bench_direct_single(single_commits=False)
    total = time() - start
    print(f"Direct, main thread, no single commits: {total} s")

    start = time()
    bench_direct(single_commits=False)
    total = time() - start
    print(f"Direct, 10 threads, no single commits: {total} s")

    start = time()
    bench_queue(single_commits=False)
    total = time() - start
    print(f"Queue, no single commits: {total} s")

    start = time()
    bench_queue_transaction()
    total = time() - start
    print(f"Queue, transactions: {total} s")

    start = time()
    bench_queue_many()
    total = time() - start
    print(f"Queue, executemany: {total} s")

    start = time()
    bench_direct_many()
    total = time() - start
    print(f"Direct, main thread, executemany: {total} s")

    start = time()
    bench_direct_single(single_commits=True)
    total = time() - start
    print(f"Direct, main thread, single commits: {total} s")

    start = time()
    bench_direct(single_commits=True)
    total = time() - start
    print(f"Direct, 10 threads, single commits: {total} s")

    start = time()
    bench_queue(single_commits=True)
    total = time() - start
    print(f"Queue, single commits: {total} s")
