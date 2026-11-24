"""Minimal synchronous libpq adapter, plus the explicit send-without-read case."""
import ctypes as C
import json


class SqlError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


class Libpq:
    def __init__(self, path):
        self.lib = C.CDLL(str(path))
        ptr, txt, integer = C.c_void_p, C.c_char_p, C.c_int
        signatures = {
            'PQconnectdbParams': ([C.POINTER(txt), C.POINTER(txt), integer], ptr),
            'PQstatus': ([ptr], integer), 'PQbackendPID': ([ptr], integer),
            'PQerrorMessage': ([ptr], txt), 'PQfinish': ([ptr], None),
            'PQexec': ([ptr,txt], ptr), 'PQclear': ([ptr], None),
            'PQresultStatus': ([ptr], integer), 'PQresultErrorField': ([ptr,integer], txt),
            'PQresultErrorMessage': ([ptr], txt), 'PQntuples': ([ptr], integer),
            'PQnfields': ([ptr], integer), 'PQgetisnull': ([ptr,integer,integer], integer),
            'PQgetvalue': ([ptr,integer,integer], txt), 'PQsendQuery': ([ptr,txt], integer),
            'PQflush': ([ptr], integer),
        }
        for name,(arguments,result) in signatures.items():
            fun = getattr(self.lib,name)
            fun.argtypes,fun.restype = arguments,result


class Connection:
    def __init__(self, pq, socket, port, name):
        self.lib=pq.lib
        keys=('host','port','dbname','user','application_name','connect_timeout')
        values=(str(socket),str(port),'postgres','course_owner',name,'5')
        vector=C.c_char_p*(len(keys)+1)
        self.ptr=self.lib.PQconnectdbParams(vector(*(s.encode() for s in keys)),
                                           vector(*(s.encode() for s in values)),0)
        if not self.ptr or self.lib.PQstatus(self.ptr)!=0:
            error=self.lib.PQerrorMessage(self.ptr).decode(errors='replace')
            self.close()
            raise RuntimeError(error)
        self.pid=self.lib.PQbackendPID(self.ptr)
        self.run("SET statement_timeout='15s'; SET lock_timeout='12s'")

    def run(self, sql):
        res=self.lib.PQexec(self.ptr,sql.encode())
        if not res:raise RuntimeError(self.lib.PQerrorMessage(self.ptr).decode())
        try:
            if self.lib.PQresultStatus(res) not in (1,2):
                code=self.lib.PQresultErrorField(res,ord('C')) or b''
                raise SqlError(code.decode(),self.lib.PQresultErrorMessage(res).decode())
            return [[None if self.lib.PQgetisnull(res,i,j) else self.lib.PQgetvalue(res,i,j).decode()
                     for j in range(self.lib.PQnfields(res))] for i in range(self.lib.PQntuples(res))]
        finally:self.lib.PQclear(res)

    def scalar(self, sql):
        rows=self.run(sql)
        if len(rows)!=1 or len(rows[0])!=1:raise ValueError(f'Expected scalar for {sql}')
        return rows[0][0]

    def plan(self, sql):
        return json.loads(self.scalar('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sql))[0]['Plan']

    def send_commit_without_read(self):
        if self.lib.PQsendQuery(self.ptr,b'COMMIT')!=1 or self.lib.PQflush(self.ptr)!=0:
            raise RuntimeError('COMMIT could not be transmitted')

    def close(self):
        if self.ptr:
            self.lib.PQfinish(self.ptr)
            self.ptr=None
