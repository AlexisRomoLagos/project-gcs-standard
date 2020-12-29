import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

class Database:
  def __init__(self,client=None,cursor=None):
    self._client = client
    self._cursor = cursor

  def connect(self,host,port,user,password,database,application_name,schema='public',timeout=0,ssl_mode=None,ssl_server_ca=None,ssl_client_cert=None,ssl_client_key=None):
    try:
      options = f'-c search_path={schema} -c statement_timeout={timeout}'
      self._client = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        application_name=application_name,
        sslmode=ssl_mode,
        sslrootcert=ssl_server_ca,
        sslcert=ssl_client_cert,
        sslkey=ssl_client_key,
        options=options
      )
    except Exception as e:
      print('error:',repr(e))
      raise e

  def connect_pool(self,host,port,user,password,database,application_name,schema='public'):
    try:
      options = f'-c search_path={schema}'
      pool = SimpleConnectionPool(
        minconn=1,
        maxconn=1,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        application_name=application_name,
        options=options
      )
      self._client = pool.getconn()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def disconnect(self):
    if self._client: self._client.close()
    if self._cursor: self._cursor.close()

  def get_client(self):
    return self._client

  def get_cursor(self):
    return self._cursor

  def load_config(self):
    self._client.autocommit = False

  def set_cursor(self):
    self._cursor = self._client.cursor(cursor_factory=RealDictCursor)