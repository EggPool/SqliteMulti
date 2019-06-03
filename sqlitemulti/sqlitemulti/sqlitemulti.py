# -*- coding: utf-8 -*-

"""
Main module.

SqliteMulti classes, sqlite3 wrapped for threadsafe calls and serialization.
"""

# from abc import ABC, abstractmethod
import sqlite3
from threading import Thread
from queue import Queue
from multiprocessing import Process, Manager
from enum import Enum


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
    timeout=None,
    isolation_level=None,
    factory=None,
    uri=None,
    verbose: bool = False,
):
    """Worker, running in thread or Process"""
    try:
        db = sqlite3.connect(database, timeout, isolation_level, factory, uri)
    except Exception as e:
        if verbose:
            print("DB Process error at connect {e}")
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
                db.execute(sql, params)
            elif command == SqlCommand.INSERT:
                # Execute and sends inserted back
                db.execute(sql, params)
                # TODO
                # res =
            elif command == SqlCommand.DELETE:
                # Execute and sends deleted back
                db.execute(sql, params)
                # TODO
                # res =
            elif command == SqlCommand.FETCHONE:
                # Execute and sends the value back
                result = db.execute(sql, params)
                res = result.fetchone()
            elif command == SqlCommand.FETCHALL:
                # Execute and sends the value back
                result = db.execute(sql, params)
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
                print("DB Process running {e}")


class SqliteMulti:
    """Tries to mimic sqlite3 interface as much as possible"""

    __slots__ = ("_command_queue", "_result_queues", "_worker")

    def __init__(
        self,
        database,
        timeout=None,
        isolation_level=None,
        factory=None,
        uri=None,
        own_process=False,
        verbose: bool = False,
    ):
        self._result_queues = dict()
        if own_process:
            self._command_queue = Manager().Queue()
            self._worker = Process(
                target=sqlite_worker,
                args=(
                    self._command_queue,
                    database,
                    timeout,
                    isolation_level,
                    factory,
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
                    factory,
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
        timeout=None,
        isolation_level=None,
        factory=None,
        uri=None,
        verbose: bool = False,
    ):
        """Alias to __init__, to be alike sqlite3 interface"""
        return cls(database, timeout, isolation_level, factory, uri, verbose)
