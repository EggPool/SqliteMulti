"""
Benchmark: read from several threads vs queued / thread pool / process pool
"""

import os
import sys
import random
import sqlite3
import threading
from time import time, sleep

sys.path.append("../")
from sqlitemulti.sqlitemulti import SqliteMulti


TOTAL_RECORDS = 10000

THREAD_COUNT = 20
RUN_COUNT = 20

# Create test table
SQL_CREATE = (
    "CREATE TABLE IF NOT EXISTS transactions ("
    "timestamp TEXT, address TEXT, amount TEXT)"
)

SQL_INSERT = "INSERT INTO transactions VALUES (?,?,?)"

SQL_READ = "SELECT SUM(amount) FROM transactions WHERE timestamp >= ? and timestamp <= ?"

SQL_CREATE_INDEX = (
    "CREATE INDEX index1 on transactions(timestamp)"
)

CREATE_INDEX = False  # Set to True to add the index.


DATA = []
TOTAL = 0

LOCK = threading.Lock()


def write_data():
    if os.path.isfile("benchr.db"):
        os.remove("benchr.db")
    db = sqlite3.connect("benchr.db")
    db.execute(SQL_CREATE)
    if CREATE_INDEX:
        db.execute(SQL_CREATE_INDEX)
    db.commit()
    random.seed("let do this deterministic")
    for lines in range(TOTAL_RECORDS):
        # Fake data
        line = (
            lines,
            random.randint(0, 9999999999),
            random.randint(0, 999999),  # Considering amounts as int to avoid roundup issues.
        )
        db.execute(SQL_INSERT, line)
    db.commit()


def make_request_data():
    # request indexes
    global DATA
    random.seed("let do this deterministic also")
    for runs in range(RUN_COUNT):
        run = []
        for lines in range(THREAD_COUNT):
            # Fake data. index, range
            line = (random.randint(0, TOTAL_RECORDS - 1), random.randint(0, 10))
            run.append(line)
        DATA.append(run)


def bench_sequential():
    db = sqlite3.connect("benchr.db")
    total = 0
    for run in range(RUN_COUNT):
        for line in range(THREAD_COUNT):
            data = DATA[run][line]
            res = db.execute(SQL_READ, (data[0], data[0] + data[1]))
            res = res.fetchone()[0]
            if res is None:
                res = 0
            total += res
    print(f" Total {total}")


def sqlite_reader(t_index, db, params):
    global TOTAL
    res = db.execute(SQL_READ, (params[0], params[0] + params[1]))
    res = res.fetchone()[0]
    with LOCK:
        if res is None:
            res = 0
        TOTAL += res


def bench_threads():
    global TOTAL
    TOTAL = 0
    # reads from threads, single db handler with check_same_thread=False, minimal Lock
    db = sqlite3.connect("benchr.db", check_same_thread=False)
    # No need for journal WAL, no writes here, but you can activate and see no diff.
    # db.execute("PRAGMA journal_mode = WAL")
    for i in range(RUN_COUNT):
        threads = []
        for t in range(THREAD_COUNT):
            thread = threading.Thread(
                target=sqlite_reader,
                args=(t, db, DATA[i][t]),
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()
    print(f" Total {TOTAL}")


def sqlite_reader2(t_index, params):
    global TOTAL
    db = sqlite3.connect("benchr.db", check_same_thread=False)
    res = db.execute(SQL_READ, (params[0], params[0] + params[1]))
    res = res.fetchone()[0]
    with LOCK:
        if res is None:
            res = 0
        TOTAL += res


def bench_threads2():
    global TOTAL
    TOTAL = 0
    # reads from threads, one db handler per thread, minimal Lock
    for i in range(RUN_COUNT):
        threads = []
        for t in range(THREAD_COUNT):
            thread = threading.Thread(
                target=sqlite_reader2,
                args=(t, DATA[i][t]),
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()
    print(f" Total {TOTAL}")


def sqlite_queuereader(t_index, db, params):
    global TOTAL
    res = db.fetchone(SQL_READ, (params[0], params[0] + params[1]))[0]
    with LOCK:
        if res is None:
            res = 0
        TOTAL += res


def bench_queue(multi_db):
    global TOTAL
    TOTAL = 0
    # reads from threads, one single SqliteMulti
    for i in range(RUN_COUNT):
        threads = []
        for t in range(THREAD_COUNT):
            thread = threading.Thread(
                target=sqlite_queuereader,
                args=(t, multi_db, DATA[i][t]),
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()
    print(f" Total {TOTAL}")


if __name__ == "__main__":

    start = time()
    write_data()
    total = time() - start
    print(f"Wrote data in {total} s")

    make_request_data()

    start = time()
    bench_sequential()
    total = time() - start
    print(f"Direct, sequential: {total} s")

    start = time()
    bench_threads()
    total = time() - start
    """Note that the lock (needed to calc TOTAL does not impact the speed. 
    even without lock, even with no calc, timing is similar."""
    print(f"Direct, {THREAD_COUNT} threads sharing db: {total} s")

    start = time()
    bench_threads2()
    total = time() - start
    print(f"Direct, {THREAD_COUNT} threads own db: {total} s")

    db = SqliteMulti.connect("benchr.db", own_process=False, tasks=1)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Threads=1: {total} s")
    db.stop()
    db.join()

    """There's is an overhead for initial worker creation, why the object init/release is out of the bench.
    in real apps, these will also be done once only."""
    db = SqliteMulti.connect("benchr.db", own_process=True, tasks=1)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Processes=1: {total} s")
    db.stop()
    db.join()

    db = SqliteMulti.connect("benchr.db", own_process=True, tasks=2, verbose=False)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Processes=2: {total} s")
    db.stop()
    db.join()

    db = SqliteMulti.connect("benchr.db", own_process=True, tasks=4, verbose=False)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Processes=4: {total} s")
    db.stop()
    db.join()

    db = SqliteMulti.connect("benchr.db", own_process=False, tasks=4, verbose=False)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Threads=4: {total} s")
    db.stop()
    db.join()

    db = SqliteMulti.connect("benchr.db", own_process=False, tasks=8, verbose=False)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Threads=8: {total} s")
    db.stop()
    db.join()

    db = SqliteMulti.connect("benchr.db", own_process=False, tasks=16, verbose=False)
    start = time()
    bench_queue(db)
    total = time() - start
    print(f"SqliteMulti, Threads=16: {total} s")
    db.stop()
    db.join()
