# -*- coding: utf-8 -*-
import decimal
import re, datetime
import sqlite3
from queue import Queue
from abc import abstractmethod

#import aiosqlite

from asyncpg.pool import PoolAcquireContext

class PoolException(Exception):
    pass

class Pool(object):
    def __init__(self, maxActive=5, maxWait=None, init_size=0, db_type="SQLite3", **config):
        self.__freeConns = Queue(maxActive)
        self.maxWait = maxWait
        self.db_type = db_type
        self.config = config
        if init_size > maxActive:
            init_size = maxActive
        for i in range(init_size):
            self.release(self._create_conn())
    
    def __del__(self):
        print("__del__ Pool..")
        self.releaseAll()
    
    def releaseAll(self):
        print("release Pool..")
        while self.__freeConns and not self.__freeConns.empty():
            con = self.get()
            con.release()

        self.__freeConns = None
 
    def _create_conn(self):
        if self.db_type in dbcs:
            return dbcs[self.db_type](**self.config)
        
    def get(self, timeout=None):
        if timeout is None:
            timeout = self.maxWait
        conn = None
        if self.__freeConns.empty():
            conn = self._create_conn()
        else:
            conn = self.__freeConns.get(timeout=timeout)
        conn.pool = self
        return conn
    
    async def release(self, conn):
        conn.pool = None
        if(self.__freeConns.full()):
            conn.release()
            return
        self.__freeConns.put_nowait(conn)

    async def _acquire(self, timeout):
        return self.get(timeout)

    def acquire(self, timeout=None):
        return PoolAcquireContext(self, timeout)

class PoolingConnection(object):
    def __init__(self, **config):
        self.conn = None
        self.config = config
        
    def __del__(self):
        self.release()
        
    def __enter__(self):
        pass
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def release(self):
        print("release PoolingConnection..")
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            
    def close(self):
        print("close")
        
    def __getattr__(self, val):
        if self.conn is None and self.pool is not None:
            self.conn = self._create_conn(**self.config)
        if self.conn is None:
            raise PoolException("PoolingConnection Error!")
        return getattr(self.conn, val)
 
    @abstractmethod
    def _create_conn(self, **config):
        pass

class SQLit3PoolConnection(PoolingConnection):

    def _create_conn(self, **config):
        con = sqlite3.connect(**config, detect_types=sqlite3.PARSE_DECLTYPES)
        con.row_factory = sqlite3.Row
        return con

    @property
    def Conn(self):
        if self.conn is None:
            self.conn = self._create_conn(**self.config)
        return self.conn

    def __enter__(self):
        if self.conn is None:
            self.conn = self._create_conn(**self.config)
        return self.conn.cursor()

    async def fetch(self, query, *args, timeout=None) -> list:
        #print("fetch ->\n", query, *args)
        sql = self._formatQuerySymbol(query)
        cur = self.Conn.execute(sql, args)
        return cur.fetchall()

    async def fetchval(self, query, *args, column=0, timeout=None):
        #print("fetchval ->\n", query, args)
        sql = self._formatQuerySymbol(query)
        cur = self.Conn.execute(sql, args)
        all = cur.fetchall()
        if len(all) <= 0 : return 
        rows = all[column]
        if len(rows) > 0:
            return rows[0]
        return 

    async def fetchrow(self, query, *args, timeout=None):
        #print("fetchrow ->\n", query, args)
        sql = self._formatQuerySymbol(query)
        cur = self.Conn.execute(sql, args)
        row = cur.fetchone()
        return row

    async def execute(self, query: str, *args, timeout: float=None) -> str:
        #print("execute ->", query, args)
        sql = self._formatQuerySymbol(query)
        cu = self.Conn.execute(sql, args)
        self.Conn.commit()
        return str(cu.rowcount)

    async def executemany(self, query: str, *args, timeout: float=None) -> str:
        #print("executemany ->\n" , query, args)
        sql = self._formatQuerySymbol(query)

        cu = self.Conn.executemany(sql, *args)
        self.Conn.commit()
        return str(cu.rowcount)
        
    def _converKeyword(self, query) -> str:
        return query

        keywords = {
            "index" : "'index'",
        }
        sql = re.sub(" index", "'index'", query)
        sql = query
        return sql

    def _converArgs(self, *args) -> tuple:
        newArgs = []
        
        for arg in args:
            if isinstance(arg, list):
                param = str(arg)
            elif isinstance(arg, tuple):
                param = str(arg)
            else:
                param = arg
            newArgs.append(param)
        return tuple(newArgs)         

    def _formatQuerySymbol(self, query) -> str:
        sql = re.sub('\$\d+', "?", self._converKeyword(query))
        #print("sql: " + sql)
        return sql

dbcs = {"SQLite3": SQLit3PoolConnection}
 

#DECTEXT type
def adapt_decimal(d):
    return str(d)

def convert_decimal(s):
    return decimal.Decimal(s.decode())

# Register the adapter
sqlite3.register_adapter(decimal.Decimal, adapt_decimal)

# Register the converter
sqlite3.register_converter("DECTEXT", convert_decimal)


#datetime.datetime type
def adapt_datetime(d):
    return str(d)

def convert_datetime(s):
    return datetime.datetime.fromisoformat(s.decode())

sqlite3.register_adapter(datetime.datetime, adapt_datetime)

sqlite3.register_converter("DATETIME", convert_datetime)

#TEXT[] type
def adapt_arrayAddress(addresses):
    return str(addresses)

sqlite3.register_adapter(list, adapt_arrayAddress)
sqlite3.register_adapter(tuple, adapt_arrayAddress)

def convert_text_array(s):
    return eval(s.decode())

sqlite3.register_converter("TEXT[]", convert_text_array)

