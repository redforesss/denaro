# -*- coding: utf-8 -*-
import decimal
import re, datetime, io, sys
import sqlite3
from queue import Queue
from abc import abstractmethod

#import aiosqlite

from asyncpg.pool import PoolAcquireContext

class PoolException(Exception):
    pass

class Pool(object):
    '''一个数据库连接池'''
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
        '''释放资源，关闭池中的所有连接'''
        print("release Pool..")
        while self.__freeConns and not self.__freeConns.empty():
            con = self.get()
            con.release()

        self.__freeConns = None
 
    def _create_conn(self):
        '''创建连接 '''
        if self.db_type in dbcs:
            return dbcs[self.db_type](**self.config);
        
    def get(self, timeout=None):
        '''获取一个连接
        @param timeout:超时时间
        '''
        if timeout is None:
            timeout = self.maxWait
        conn = None
        if self.__freeConns.empty():#如果容器是空的，直接创建一个连接
            conn = self._create_conn()
        else:
            conn = self.__freeConns.get(timeout=timeout)
        conn.pool = self
        return conn
    
    async def release(self, conn):
        '''将一个连接放回池中
        @param conn: 连接对象
        '''
        conn.pool = None
        if(self.__freeConns.full()):#如果当前连接池已满，直接关闭连接
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
            raise PoolException("无法创建数据库连接 或连接已关闭")
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
        #print("fetch")
        sql = self._formatQuerySymbol(query)
        parameters = self._converArgs(*args)
        
        cur = self.Conn.execute(sql, parameters)
        return cur.fetchall()

    async def fetchval(self, query, *args, column=0, timeout=None):
        #print("fetchval ")
        sql = self._formatQuerySymbol(query)
        parameters = self._converArgs(*args)
        cur = self.Conn.execute(sql, parameters)
        all = cur.fetchall()
        if len(all) <= 0 : return 
        rows = all[column]
        if len(rows) > 0:
            return rows[0]
        return 

    async def fetchrow(self, query, *args, timeout=None):
        #print("fetchrow ")
        sql = self._formatQuerySymbol(query)
        parameters = self._converArgs(*args)
        cur = self.Conn.execute(sql, parameters)
        row = cur.fetchone()
        return row

    async def execute(self, query: str, *args, timeout: float=None) -> str:
        #print("execute ->", query, args)
        sql = self._formatQuerySymbol(query)
        parameters = self._converArgs(*args)
        cu = self.Conn.execute(sql, parameters)
        self.Conn.commit()
        return str(cu.rowcount)

    async def executemany(self, query: str, *args, timeout: float=None) -> str:
        #print("executemany ->\n" , query, args)
        sql = self._formatQuerySymbol(query)
        #parameters = map(lambda x : self._converArgs(x), args)
        #print("parameters", parameters)
        cu = self.Conn.executemany(sql, *args)
        self.Conn.commit()
        return str(cu.rowcount)
        
    def _converKeyword(self, query) -> str:
        keywords = {
            "index" : "'index'",
        }
        #sql = re.sub(" index", "'index'", query)
        sql = query
        return sql

    def _converArgs(self, *args) -> tuple:
        newArgs = []
        
        for arg in args:
            if type(arg) is datetime.datetime:
                param = arg
            elif type(arg) is str:
                param = arg
            elif type(arg) is int:
                param = arg
            elif type(arg) is decimal.Decimal:
                param = arg
            else:
                param = f"'{str(arg)}'"
            newArgs.append(param)
        #print ("_converArgs \n", newArgs)
        return tuple(newArgs)
                
    def _formatQuery(self, query, *args) -> str:
        parameters = {}
        for i in range(len(args)):
            arg = args[i]
            param = f"'{str(arg)}'"
            print(f"{args[i]} -> {type(arg)}")

            if type(arg) is datetime.datetime:
                param = str(int(arg.timestamp()))
            if type(arg) is list:
                param = arg
            
            parameters[f'${i+1}'] = param
        print (parameters)

        sql = re.sub('\$\d+', lambda x:parameters[x.group()], self._converKeyword(query))
        print ("sql: " + sql)
        return sql

    def _formatQuerySymbol(self, query) -> str:
        sql = re.sub('\$\d+', "?", self._converKeyword(query))
        #print("sql: " + sql)
        return sql

dbcs = {"SQLite3": SQLit3PoolConnection}
 


# def adapt_arrayAddress(addresses):
#     out = io.BytesIO()
#     np.save(out, arr)
#     out.seek(0)
#     return sqlite3.Binary(out.read())

# def convert_array(text):
#     out = io.BytesIO(text)
#     out.seek(0)
#     return np.load(out)

# sqlite3.register_adapter(list(str), adapt_arrayAddress)

# sqlite3.register_converter("array", convert_array)

def adapt_decimal(d):
    return str(d)

def convert_decimal(s):
    return decimal.Decimal(s.decode())

# Register the adapter
sqlite3.register_adapter(decimal.Decimal, adapt_decimal)

# Register the converter
sqlite3.register_converter("DECTEXT", convert_decimal)

def adapt_datetime(d):
    return d.timestamp()

def convert_datetime(s):
    return datetime.datetime.fromtimestamp(int(s.decode()))

# Register the adapter
sqlite3.register_adapter(datetime.datetime, adapt_datetime)

# Register the converter
sqlite3.register_converter("DATETIME", convert_datetime)


async def test():
    pool = Pool(database="./product2.db")

    with pool.get() as cu:
        #cu = conn.cursor()
        # cu.execute("create table lang(name, first_appeared)")

        s = '''INSERT INTO transactions (block_hash, tx_hash, tx_hex, inputs_addresses, fees) VALUES (?, ?, ?, ?, ?)'''
        s2 = "select * FROM unspent_outputs WHERE (tx_hash, _index) in ( values ('8376a6b4a41fb3c797c216babc5648b63afbec2de1196c4e38406c96ef98f418', 0), ('8376a6b4a41fb3c797c216babc5648b63afbec2de1196c4e38406c96ef98f418', 0))"
        s3 = "INSERT INTO unspent_outputs (tx_hash, _index) VALUES ($1, $2) ([('a8b53282ce031172be80aa1ce9787d9979efe38d1f28d6bd402c0d2b05e86fee', 0)],)"
        args =   "('bfe9ebf617b99b04042ad5eeebe741168e55e33cd59699b5b5fe4737109403b5', '40682e6d2ab1a03ce46e082a92cc265d58eb4ca23ac9f849755240e82d267747')"

        #cu.executemany(s3, [])
        #cu.execute(s2)
        # cu.execute(s, args)
        # cu.connection.commit()
        #cu.execute("insert into lang values (?, ?)", ("A", 1988))
        
        # for row in cu.execute("select * from blocks"):
        #     print(row.keys())
        #     print(tuple(row))

    test_get_transaction_by_contains_multi()

def test_get_transaction_by_contains_multi():
    pool = Pool(database="./product2.db")
    with pool.get() as cu:
        contains = ['01011', "01012"]
        fs = [f'tx_hex LIKE "%{contains}%"' for contains in contains]

        print(" or ".join([f'tx_hex LIKE "%{contains}%"' for contains in contains]))
        ignore = "bfe9ebf617b99b04042ad5eeebe741168e55e33cd59699b5b5fe4737109403b5"
        sql = f'''SELECT tx_hash FROM transactions WHERE ({str(" or ".join([f'tx_hex LIKE "%{contains}%"' for contains in contains]))}) AND tx_hash != ? LIMIT 1'''
        print (sql)
        ret = cu.execute(sql, [ignore])
        print(tuple(ret.fetchone()))

if __name__ == "__main__":
    import asyncio
    asyncio.run(test())