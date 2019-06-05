# -*- coding: utf-8 -*-

"""
Main module.

SqliteMulti classes, sqlite3 wrapped for threadsafe calls and serialization.
"""

import sqlite3
from threading import Thread, get_ident
from queue import Queue
from multiprocessing import Process, Manager
from enum import Enum
from time import time


# When a queue is that old and empty, delete it
OLD_QUEUE_TRIGGER = 60 * 5


class SqlCommand(Enum):
    EXECUTE = 1
    INSERT = 2
    DELETE = 3
    FETCHONE = 4
    FETCHALL = 5
    COMMIT = 6
    STOP = 7


def sqlite_worker(
    queue,
    database,
    timeout=5,
    isolation_level=None,
    uri=None,
    verbose: bool = False,
):
    """Worker, running in thread or Process"""
    try:
        # print("isolation", isolation_level)
        db = sqlite3.connect(database, timeout=timeout, isolation_level=isolation_level, uri=uri, check_same_thread=False)
    except Exception as e:
        if verbose:
            print(f"DB Process error at connect {e}")
        raise
    if verbose:
        print(f"DB Queue {database} started")
    while True:
        try:
            result_queue, command, sql, params, commit = queue.get(
                block=True, timeout=30
            )
            if verbose:
                print(f"DB Queue got {command}:{sql} {params}")
            res = None
            if command == SqlCommand.EXECUTE:
                # Execute and sends None back
                if params:
                    db.execute(sql, params)
                else:
                    db.execute(sql)
            elif command == SqlCommand.INSERT:
                # Execute and sends inserted back
                if params:
                    db.execute(sql, params)
                else:
                    db.execute(sql)
                # TODO
                # res =
            elif command == SqlCommand.DELETE:
                # Execute and sends deleted back
                if params:
                    db.execute(sql, params)
                else:
                    db.execute(sql)
                # TODO
                # res =
            elif command == SqlCommand.FETCHONE:
                # Execute and sends the value back
                if params:
                    result = db.execute(sql, params)
                else:
                    result = db.execute(sql)
                res = result.fetchone()
            elif command == SqlCommand.FETCHALL:
                # Execute and sends the value back
                if params:
                    result = db.execute(sql, params)
                else:
                    result = db.execute(sql)
                res = result.fetchall()
            elif command == SqlCommand.COMMIT:
                db.commit()
            elif command == SqlCommand.STOP:
                if verbose:
                    print("DB Process stopping")
                return
            else:
                raise ValueError("Unknown SqlCommand")
            if commit:
                db.commit()
            # Send the data back to the provided queue
            if result_queue:
                result_queue.put(res)
            queue.task_done()
        except Exception as e:
            if verbose:
                print(f"DB Process running {e}")


class SqliteMulti:
    """Tries to mimic sqlite3 interface as much as possible"""

    __slots__ = ("_command_queue", "_own_process", "_result_queues", "_worker", "_verbose", "_stopping")

    def __init__(
        self,
        database,
        timeout=5,
        isolation_level=None,
        uri=None,
        own_process=False,
        verbose: bool=False,
    ):
        self._result_queues = dict()
        self._own_process = own_process
        self._verbose = verbose
        self._stopping = False
        if verbose:
            print("__Init__")
        if own_process:
            self._command_queue = Manager().Queue()
            self._worker = Process(
                target=sqlite_worker,
                args=(
                    self._command_queue,
                    database,
                    timeout,
                    isolation_level,
                    uri,
                    verbose,
                ),
            )
            self._worker.daemon = False
        else:
            self._command_queue = Queue()
            self._worker = Thread(
                target=sqlite_worker,
                args=(
                    self._command_queue,
                    database,
                    timeout,
                    isolation_level,
                    uri,
                    verbose,
                ),
            )
            self._worker.daemon = True
        self._worker.start()

    @classmethod
    def connect(
        cls,
        database,
        timeout=5,
        isolation_level=None,
        uri=None,
        own_process=False,
        verbose: bool = False,
    ):
        """Alias to __init__, to be alike sqlite3 interface"""
        return cls(database, timeout, isolation_level, uri, own_process, verbose)

    def status(self) -> str:
        """Returns a status of current queues occupation"""
        try:
            status = f"{self._command_queue.qsize()} commands\n"
        except:
            # Note that this may raise NotImplementedError on Unix platforms like Mac OS X where sem_getvalue() is not implemented.
            pass
        status += f"{len(self._result_queues)} result queues\n"
        try:
            for id, queue in self._result_queues.items():
                status += f"  {id}: {queue[0].qsize()}\n"
        except:
            pass
        return status

    def stop(self):
        """Signal the worker to end"""
        if self._verbose:
            print("Stop required")
        self._stopping = True
        self._command_queue.put((None, SqlCommand.STOP, '', None, False))

    def join(self):
        """Waits until the worker ends nicely. only to be called after a stop(), or will never return"""
        if self._verbose:
            print("Join required")
        if not self._stopping:
            raise RuntimeError("Join was required, but no stop() before")
        self._worker.join()

    def _execute(self, command: SqlCommand, sql: str, params=None, commit=False):
        """Generic queued command. Enqueues the request, and waits for the answer."""
        thread_id = str(get_ident())
        if self._verbose:
            print(f"Called from thread {thread_id}")
        if thread_id in self._result_queues:
            # we had a queue, is it still good? - we need some garbage collector to delete old queues
            if self._verbose:
                print("TODO - check thread")
            result_queue = self._result_queues[thread_id][0]
        else:
            if self._own_process:
                result_queue = Manager().Queue()
            else:
                result_queue = Queue()
            self._result_queues[thread_id] = (result_queue, time() + OLD_QUEUE_TRIGGER)
        # Enqueue the command
        self._command_queue.put((result_queue, command, sql, params, commit))
        # And wait for its answer
        if self._verbose:
            print("Waiting...")
        res = result_queue.get()
        result_queue.task_done()
        return res

    def commit(self):
        """Signal the worker to commit"""
        return self._execute(SqlCommand.COMMIT, '')

    def execute(self, sql, params=None, commit=False):
        """Emulates an execute. Enqueues the request, and waits for the answer."""
        return self._execute(SqlCommand.EXECUTE, sql, params, commit)

    def fetchall(self, sql, params=None):
        return self._execute(SqlCommand.FETCHALL, sql, params)

    def fetchone(self, sql, params=None):
        return self._execute(SqlCommand.FETCHONE, sql, params)

    def insert(self, sql, params=None, commit=True):
        """Emulates an insert. commit is True by default. Enqueues the request, and waits for the answer."""
        return self._execute(SqlCommand.INSERT, sql, params, commit)

    def delete(self, sql, params=None, commit=True):
        """Emulates a delete. commit is True by default. Enqueues the request, and waits for the answer."""
        return self._execute(SqlCommand.DELETE, sql, params, commit)
