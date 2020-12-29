from . import Database

class MwCustomersDex(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'mw_customers_dex'

  def create(self):
    try:
      sql = f'''
        create table if not exists {self.schema}.{self.table}(
          date date not null,
          id bigint not null,--Cliente DEX Cod
          name varchar(100) not null,--Cliente DEX
          type_id smallint null,--Boca de Salida DEX Cod
          type varchar(50) null,--Boca de Salida DEX
          ubigeo_id char(1) null,--Tipo de Ubigeo Cod
          ubigeo varchar(20) null,--Tipo de Ubigeo
          city_id varchar(10) null,--Ciudad Cod
          city varchar(100) null,--Ciudad
          department_id varchar(6) null,--Departamento Cod
          department varchar(100) null,--Departamento DEX
          province_id varchar(6) null,--Provincia Cod
          province varchar(100) null,--Provincia DEX
          district_id varchar(6) null,--Distrito DEX Cod
          district varchar(100) null,--Distrito DEX
          channel_id smallint null,--Canal DEX Cod
          channel varchar(50) null,--Canal DEX
          line_id varchar(2) null,--Giro del Cliente Cod (Giro del Cliente DEX Cod)
          line varchar(50) null,--Giro del Cliente (Giro del Cliente DEX)
          segment_id varchar(2) null,--Segmento DEX Cod
          segment varchar(20) null,--Segmento DEX
          module_id varchar(20) null,--Modulo Cliente DEX Cod
          module varchar(20) null,--Modulo Cliente DEX
          classification_id varchar(2) null,--Clasificación de Clientes (Análisis ABC) Cod
          classification varchar(100) null,--Clasificación de Clientes (Análisis ABC)
          payment_condition_id varchar(10) null,--Condición de Pago Cod
          payment_condition varchar(100) null--Condición de Pago
        )
        partition by range(date)
        tablespace pg_default;

        comment on table {self.schema}.{self.table} is 'Customers DEX Table: [lib://QVD''s Aux Alicorp/ZQV_ZDEXCLIE.QVD]';

        create table if not exists {self.schema}.{self.table}_202001 partition of {self.schema}.{self.table} for values from ('2020-01-01') to ('2020-02-01');
        create table if not exists {self.schema}.{self.table}_202002 partition of {self.schema}.{self.table} for values from ('2020-02-01') to ('2020-03-01');
        create table if not exists {self.schema}.{self.table}_202003 partition of {self.schema}.{self.table} for values from ('2020-03-01') to ('2020-04-01');
        create table if not exists {self.schema}.{self.table}_202004 partition of {self.schema}.{self.table} for values from ('2020-04-01') to ('2020-05-01');
        create table if not exists {self.schema}.{self.table}_202005 partition of {self.schema}.{self.table} for values from ('2020-05-01') to ('2020-06-01');
        create table if not exists {self.schema}.{self.table}_202006 partition of {self.schema}.{self.table} for values from ('2020-06-01') to ('2020-07-01');
        create table if not exists {self.schema}.{self.table}_202007 partition of {self.schema}.{self.table} for values from ('2020-07-01') to ('2020-08-01');
        create table if not exists {self.schema}.{self.table}_202008 partition of {self.schema}.{self.table} for values from ('2020-08-01') to ('2020-09-01');
        create table if not exists {self.schema}.{self.table}_202009 partition of {self.schema}.{self.table} for values from ('2020-09-01') to ('2020-10-01');
        create table if not exists {self.schema}.{self.table}_202010 partition of {self.schema}.{self.table} for values from ('2020-10-01') to ('2020-11-01');
        create table if not exists {self.schema}.{self.table}_202011 partition of {self.schema}.{self.table} for values from ('2020-11-01') to ('2020-12-01');
        create table if not exists {self.schema}.{self.table}_202012 partition of {self.schema}.{self.table} for values from ('2020-12-01') to ('2021-01-01');

        create table if not exists {self.schema}.{self.table}_202101 partition of {self.schema}.{self.table} for values from ('2021-01-01') to ('2021-02-01');
        create table if not exists {self.schema}.{self.table}_202102 partition of {self.schema}.{self.table} for values from ('2021-02-01') to ('2021-03-01');
        create table if not exists {self.schema}.{self.table}_202103 partition of {self.schema}.{self.table} for values from ('2021-03-01') to ('2021-04-01');
        create table if not exists {self.schema}.{self.table}_202104 partition of {self.schema}.{self.table} for values from ('2021-04-01') to ('2021-05-01');
        create table if not exists {self.schema}.{self.table}_202105 partition of {self.schema}.{self.table} for values from ('2021-05-01') to ('2021-06-01');
        create table if not exists {self.schema}.{self.table}_202106 partition of {self.schema}.{self.table} for values from ('2021-06-01') to ('2021-07-01');
        create table if not exists {self.schema}.{self.table}_202107 partition of {self.schema}.{self.table} for values from ('2021-07-01') to ('2021-08-01');
        create table if not exists {self.schema}.{self.table}_202108 partition of {self.schema}.{self.table} for values from ('2021-08-01') to ('2021-09-01');
        create table if not exists {self.schema}.{self.table}_202109 partition of {self.schema}.{self.table} for values from ('2021-09-01') to ('2021-10-01');
        create table if not exists {self.schema}.{self.table}_202110 partition of {self.schema}.{self.table} for values from ('2021-10-01') to ('2021-11-01');
        create table if not exists {self.schema}.{self.table}_202111 partition of {self.schema}.{self.table} for values from ('2021-11-01') to ('2021-12-01');
        create table if not exists {self.schema}.{self.table}_202112 partition of {self.schema}.{self.table} for values from ('2021-12-01') to ('2022-01-01');

        create table if not exists {self.schema}.{self.table}_202201 partition of {self.schema}.{self.table} for values from ('2022-01-01') to ('2022-02-01');
        create table if not exists {self.schema}.{self.table}_202202 partition of {self.schema}.{self.table} for values from ('2022-02-01') to ('2022-03-01');
        create table if not exists {self.schema}.{self.table}_202203 partition of {self.schema}.{self.table} for values from ('2022-03-01') to ('2022-04-01');
        create table if not exists {self.schema}.{self.table}_202204 partition of {self.schema}.{self.table} for values from ('2022-04-01') to ('2022-05-01');
        create table if not exists {self.schema}.{self.table}_202205 partition of {self.schema}.{self.table} for values from ('2022-05-01') to ('2022-06-01');
        create table if not exists {self.schema}.{self.table}_202206 partition of {self.schema}.{self.table} for values from ('2022-06-01') to ('2022-07-01');
        create table if not exists {self.schema}.{self.table}_202207 partition of {self.schema}.{self.table} for values from ('2022-07-01') to ('2022-08-01');
        create table if not exists {self.schema}.{self.table}_202208 partition of {self.schema}.{self.table} for values from ('2022-08-01') to ('2022-09-01');
        create table if not exists {self.schema}.{self.table}_202209 partition of {self.schema}.{self.table} for values from ('2022-09-01') to ('2022-10-01');
        create table if not exists {self.schema}.{self.table}_202210 partition of {self.schema}.{self.table} for values from ('2022-10-01') to ('2022-11-01');
        create table if not exists {self.schema}.{self.table}_202211 partition of {self.schema}.{self.table} for values from ('2022-11-01') to ('2022-12-01');
        create table if not exists {self.schema}.{self.table}_202212 partition of {self.schema}.{self.table} for values from ('2022-12-01') to ('2023-01-01');

        create table if not exists {self.schema}.{self.table}_202301 partition of {self.schema}.{self.table} for values from ('2023-01-01') to ('2023-02-01');
        create table if not exists {self.schema}.{self.table}_202302 partition of {self.schema}.{self.table} for values from ('2023-02-01') to ('2023-03-01');
        create table if not exists {self.schema}.{self.table}_202303 partition of {self.schema}.{self.table} for values from ('2023-03-01') to ('2023-04-01');
        create table if not exists {self.schema}.{self.table}_202304 partition of {self.schema}.{self.table} for values from ('2023-04-01') to ('2023-05-01');
        create table if not exists {self.schema}.{self.table}_202305 partition of {self.schema}.{self.table} for values from ('2023-05-01') to ('2023-06-01');
        create table if not exists {self.schema}.{self.table}_202306 partition of {self.schema}.{self.table} for values from ('2023-06-01') to ('2023-07-01');
        create table if not exists {self.schema}.{self.table}_202307 partition of {self.schema}.{self.table} for values from ('2023-07-01') to ('2023-08-01');
        create table if not exists {self.schema}.{self.table}_202308 partition of {self.schema}.{self.table} for values from ('2023-08-01') to ('2023-09-01');
        create table if not exists {self.schema}.{self.table}_202309 partition of {self.schema}.{self.table} for values from ('2023-09-01') to ('2023-10-01');
        create table if not exists {self.schema}.{self.table}_202310 partition of {self.schema}.{self.table} for values from ('2023-10-01') to ('2023-11-01');
        create table if not exists {self.schema}.{self.table}_202311 partition of {self.schema}.{self.table} for values from ('2023-11-01') to ('2023-12-01');
        create table if not exists {self.schema}.{self.table}_202312 partition of {self.schema}.{self.table} for values from ('2023-12-01') to ('2024-01-01');

        create table if not exists {self.schema}.{self.table}_202401 partition of {self.schema}.{self.table} for values from ('2024-01-01') to ('2024-02-01');
        create table if not exists {self.schema}.{self.table}_202402 partition of {self.schema}.{self.table} for values from ('2024-02-01') to ('2024-03-01');
        create table if not exists {self.schema}.{self.table}_202403 partition of {self.schema}.{self.table} for values from ('2024-03-01') to ('2024-04-01');
        create table if not exists {self.schema}.{self.table}_202404 partition of {self.schema}.{self.table} for values from ('2024-04-01') to ('2024-05-01');
        create table if not exists {self.schema}.{self.table}_202405 partition of {self.schema}.{self.table} for values from ('2024-05-01') to ('2024-06-01');
        create table if not exists {self.schema}.{self.table}_202406 partition of {self.schema}.{self.table} for values from ('2024-06-01') to ('2024-07-01');
        create table if not exists {self.schema}.{self.table}_202407 partition of {self.schema}.{self.table} for values from ('2024-07-01') to ('2024-08-01');
        create table if not exists {self.schema}.{self.table}_202408 partition of {self.schema}.{self.table} for values from ('2024-08-01') to ('2024-09-01');
        create table if not exists {self.schema}.{self.table}_202409 partition of {self.schema}.{self.table} for values from ('2024-09-01') to ('2024-10-01');
        create table if not exists {self.schema}.{self.table}_202410 partition of {self.schema}.{self.table} for values from ('2024-10-01') to ('2024-11-01');
        create table if not exists {self.schema}.{self.table}_202411 partition of {self.schema}.{self.table} for values from ('2024-11-01') to ('2024-12-01');
        create table if not exists {self.schema}.{self.table}_202412 partition of {self.schema}.{self.table} for values from ('2024-12-01') to ('2025-01-01');

        create table if not exists {self.schema}.{self.table}_202501 partition of {self.schema}.{self.table} for values from ('2025-01-01') to ('2025-02-01');
        create table if not exists {self.schema}.{self.table}_202502 partition of {self.schema}.{self.table} for values from ('2025-02-01') to ('2025-03-01');
        create table if not exists {self.schema}.{self.table}_202503 partition of {self.schema}.{self.table} for values from ('2025-03-01') to ('2025-04-01');
        create table if not exists {self.schema}.{self.table}_202504 partition of {self.schema}.{self.table} for values from ('2025-04-01') to ('2025-05-01');
        create table if not exists {self.schema}.{self.table}_202505 partition of {self.schema}.{self.table} for values from ('2025-05-01') to ('2025-06-01');
        create table if not exists {self.schema}.{self.table}_202506 partition of {self.schema}.{self.table} for values from ('2025-06-01') to ('2025-07-01');
        create table if not exists {self.schema}.{self.table}_202507 partition of {self.schema}.{self.table} for values from ('2025-07-01') to ('2025-08-01');
        create table if not exists {self.schema}.{self.table}_202508 partition of {self.schema}.{self.table} for values from ('2025-08-01') to ('2025-09-01');
        create table if not exists {self.schema}.{self.table}_202509 partition of {self.schema}.{self.table} for values from ('2025-09-01') to ('2025-10-01');
        create table if not exists {self.schema}.{self.table}_202510 partition of {self.schema}.{self.table} for values from ('2025-10-01') to ('2025-11-01');
        create table if not exists {self.schema}.{self.table}_202511 partition of {self.schema}.{self.table} for values from ('2025-11-01') to ('2025-12-01');
        create table if not exists {self.schema}.{self.table}_202512 partition of {self.schema}.{self.table} for values from ('2025-12-01') to ('2026-01-01');

        do
        $$begin
        if not exists(select constraint_name
                      from information_schema.table_constraints
                      where table_schema='{self.schema}' and
                        table_name='{self.table}' and
                        constraint_name='{self.schema}_{self.table}_pk')
        then
          alter table {self.schema}.{self.table}
            add constraint {self.schema}_{self.table}_pk primary key(date,id);
        end if;
        end$$;

        do
        $$begin
        if not exists(
          select indexname
          from pg_catalog.pg_indexes
          where schemaname='{self.schema}' and
            tablename like '{self.table}%'
          limit 1
        ) then
          create index idx_{self.schema}_{self.table}_1 on {self.schema}.{self.table} using btree(id);
        end if;
        end$$;
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def clean(self,date):
    try:
      sql = f'''
        delete from
          {self.schema}.{self.table}
        where
          date = '{date}';
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def load(self,date):
    try:
      sql = f'''
        insert into {self.schema}.{self.table}
        select '{date}',*
        from {self.schema}.{self.table.replace('mw_','mu_')}
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e