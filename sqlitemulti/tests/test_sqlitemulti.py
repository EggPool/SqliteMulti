#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `sqlitemulti` package.

execute all tests by running "pytest -v" in the test repo, or launch each .py to get a more verbose output of a subset of tests only.
"""

import pytest
import os
import sys

sys.path.append("../")
from sqlitemulti.sqlitemulti import SqliteMulti


# Create mempool table
SQL_CREATE = (
    "CREATE TABLE IF NOT EXISTS transactions ("
    "timestamp TEXT, address TEXT, recipient TEXT, amount TEXT, signature TEXT, "
    "public_key TEXT, operation TEXT, openfield TEXT, mergedts INTEGER)"
)


def test_connect_db():
    if os.path.isfile("test.db"):
        os.remove("test.db")
    db = SqliteMulti.connect("test.db")


def test_create_table():
    if os.path.isfile("test.db"):
        os.remove("test.db")
    db = SqliteMulti.connect("test.db")
    db.execute(SQL_CREATE, commit=True)  # Will do sql + commit
    # reread
    res = db.fetchall("PRAGMA table_info('transactions')")
    assert (
        str(res)
        == "[(0, 'timestamp', 'TEXT', 0, None, 0), (1, 'address', 'TEXT', 0, None, 0), (2, 'recipient', 'TEXT', 0, None, 0), (3, 'amount', 'TEXT', 0, None, 0), (4, 'signature', 'TEXT', 0, None, 0), (5, 'public_key', 'TEXT', 0, None, 0), (6, 'operation', 'TEXT', 0, None, 0), (7, 'openfield', 'TEXT', 0, None, 0), (8, 'mergedts', 'INTEGER', 0, None, 0)]"
    )


if __name__ == "__main__":
    if os.path.isfile("test.db"):
        os.remove("test.db")
    db = SqliteMulti.connect("test.db", verbose=True)
    db.execute("PRAGMA journal_mode = WAL")
    db.execute(SQL_CREATE, commit=True)  # Will do sql + commit
    # reread
    res = db.fetchall("PRAGMA table_info('transactions')")
    print(res)
    assert (
        str(res)
        == "[(0, 'timestamp', 'TEXT', 0, None, 0), (1, 'address', 'TEXT', 0, None, 0), (2, 'recipient', 'TEXT', 0, None, 0), (3, 'amount', 'TEXT', 0, None, 0), (4, 'signature', 'TEXT', 0, None, 0), (5, 'public_key', 'TEXT', 0, None, 0), (6, 'operation', 'TEXT', 0, None, 0), (7, 'openfield', 'TEXT', 0, None, 0), (8, 'mergedts', 'INTEGER', 0, None, 0)]"
    )

    db.stop()
    print(db.status())
    #  Wait for end
    db.join()
