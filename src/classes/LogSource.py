from . import Database

class LogSource(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'prd'
    self.name = 'log_sources'

  def find_by_bucket_name(self,p):
    try:
      sql = f'''
        select *
        from {self.schema}.{self.name}
        where bucket = '{p['bucket']}' and name = '{p['name']}';
      '''
      self._cursor.execute(sql)
      result = self._cursor.fetchone()
      return result
    except Exception as e:
      print('error:',repr(e))
      raise e