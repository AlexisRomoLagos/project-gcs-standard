from . import Database

class MuTerritoriesDex(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'mu_territories_dex'

  def create_raw(self):
    try:
      sql = f'''
        drop table if exists raw.{self.schema}_{self.table};
        create unlogged table raw.{self.schema}_{self.table}(
          "Territorio DEX Cod" varchar(4000) null,
          "Fuerza de Ventas DEX Cod" varchar(4000) null,
          "Territorio DEX" varchar(4000) null,
          "Fuerza de Ventas DEX" varchar(4000) null
        ) with(
          OIDS = false
        )
        tablespace pg_default;
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def load_raw(self,binary_file):
    try:
      sql = f'''
        COPY raw.{self.schema}_{self.table}
        FROM stdin
        WITH (
          FORMAT 'csv',
          OIDS 'false',
          FREEZE 'false',
          DELIMITER ',',
          NULL '',
          HEADER 'true',
          QUOTE '"',
          ESCAPE '\\',
          ENCODING 'utf-8'
        )
      '''
      self._cursor.copy_expert(
        sql=sql,
        file=binary_file,
        size=8192
      )
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def create_stg(self):
    try:
      sql = f'''
        drop table if exists stg.{self.schema}_{self.table};
        create unlogged table stg.{self.schema}_{self.table}(
          id int not null,--Territorio DEX Cod
          name varchar(100) not null,--Territorio DEX
          sales_force_id varchar(20) null,--Fuerza de Ventas DEX Cod
          sales_force varchar(20) not null--Fuerza de Ventas DEX
        ) with(
          OIDS = false
        )
        tablespace pg_default;

        comment on table stg.{self.schema}_{self.table} is 'Territories DEX Table';
        comment on column stg.{self.schema}_{self.table}.id is 'Territorio DEX Cod';
        comment on column stg.{self.schema}_{self.table}.name is 'Territorio DEX';
        comment on column stg.{self.schema}_{self.table}.sales_force_id is 'Fuerza de Ventas DEX Cod';
        comment on column stg.{self.schema}_{self.table}.sales_force is 'Fuerza de Ventas DEX';
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def transformation_stg(self):
    try:
      sql = f'''
        insert into stg.{self.schema}_{self.table}
        select
          "Territorio DEX Cod"::int as id,
          upper(trim("Territorio DEX")) as name,
          upper(trim("Fuerza de Ventas DEX Cod")) as sales_force_id,
          upper(trim("Fuerza de Ventas DEX")) as sales_force
        from
          raw.{self.schema}_{self.table}
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def create(self):
    try:
      sql = f'''
        create table if not exists {self.schema}.{self.table}(like stg.{self.schema}_{self.table} including all)
          tablespace pg_default;

        comment on table {self.schema}.{self.table} is 'Territories DEX Table: [lib://QVD''s Aux Alicorp/ZQV_IO_ZDEXZUONR.QVD]';

        do
        $$begin
        if not exists(select constraint_name
                      from information_schema.table_constraints
                      where table_schema='{self.schema}' and
                        table_name='{self.table}' and
                        constraint_name='{self.schema}_{self.table}_pk')
        then
          alter table {self.schema}.{self.table}
            add constraint {self.schema}_{self.table}_pk primary key(id);
        end if;
        end$$;
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def truncate(self):
    try:
      self._cursor.execute(f'truncate {self.schema}.{self.table}; truncate prd.;')
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def load(self):
    try:
      sql = f'''
        insert into {self.schema}.{self.table}
        select *
        from stg.{self.schema}_{self.table};

        insert into prd.dm_territories_dex
        select *
        from stg.{self.schema}_{self.table};
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e