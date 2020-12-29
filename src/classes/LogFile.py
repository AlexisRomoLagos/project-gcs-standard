from . import Database

class LogFile(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'prd'
    self.name = 'log_files'

  def create(self,p):
    try:
      sql = f'''
        insert into {self.schema}.{self.name}(
          source_id,
          bucket,
          filename,
          date,
          trigger,
          rows,
          status
        )
        values(
          {p['source_id']},
          '{p['bucket']}',
          '{p['filename']}',
          '{p['date']}',
          '{p['trigger']}',
          {p['rows']},
          {p['status']}
        );
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def find_by_bucket_filename(self,p):
    try:
      sql = f'''
        select *
        from {self.schema}.{self.name}
        where bucket = '{p['bucket']}' and filename = '{p['filename']}';
      '''
      self._cursor.execute(sql)
      result = self._cursor.fetchone()
      return result
    except Exception as e:
      print('error:',repr(e))
      raise e

  def update(self,p):
    try:
      rows = p['rows'] if p.get('rows') else 'rows'
      status = p['status'] if p.get('status') else 'status'
      created_at = f"'{p['created_at']}'" if p.get('created_at') else 'created_at'
      sql = f'''
        update {self.schema}.{self.name} set
          rows = {rows},
          status = {status},
          created_at = {created_at},
          updated_at = now() at time zone 'utc'
        where
          bucket = '{p['bucket']}' and
          filename = '{p['filename']}';
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e