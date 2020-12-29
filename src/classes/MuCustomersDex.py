from . import Database

class MuCustomersDex(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'mu_customers_dex'

  def create_raw(self):
    try:
      sql = f'''
        drop table if exists raw.{self.schema}_{self.table};
        create unlogged table raw.{self.schema}_{self.table}(
          "Clasificación de Clientes (Análisis ABC) Cod" varchar(4000) null,
          "Ciudad Cod" varchar(4000) null,
          "Provincia Cod" varchar(4000) null,
          "Departamento Cod" varchar(4000) null,
          "Canal DEX Cod" varchar(4000) null,
          "Cliente DEX Cod" varchar(4000) null,
          "Giro del Cliente Cod" varchar(4000) null,
          "Boca de Salida DEX Cod" varchar(4000) null,
          "Condición de Pago Cod" varchar(4000) null,
          "Distrito DEX Cod" varchar(4000) null,
          "Segmento DEX Cod" varchar(4000) null,
          "Tipo de Ubigeo Cod" varchar(4000) null,
          "Modulo Cliente DEX Cod" varchar(4000) null,
          "Clasificación de Clientes (Análisis ABC)" varchar(4000) null,
          "Ciudad" varchar(4000) null,
          "Departamento DEX" varchar(4000) null,
          "Provincia DEX" varchar(4000) null,
          "Canal DEX" varchar(4000) null,
          "Cliente DEX" varchar(4000) null,
          "Giro del Cliente" varchar(4000) null,
          "Boca de Salida DEX" varchar(4000) null,
          "Condición de Pago" varchar(4000) null,
          "Distrito DEX" varchar(4000) null,
          "Segmento DEX" varchar(4000) null,
          "Modulo Cliente DEX" varchar(4000) null,
          "Tipo de Ubigeo" varchar(4000) null
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
        with(
          OIDS = false
        )
        tablespace pg_default;

        comment on table stg.{self.schema}_{self.table} is 'Customers DEX Table';
        comment on column stg.{self.schema}_{self.table}.id is 'Cliente DEX Cod';
        comment on column stg.{self.schema}_{self.table}.name is 'Cliente DEX';
        comment on column stg.{self.schema}_{self.table}.type_id is 'Boca de Salida DEX Cod =>
2: BODEGA
4: PANADERÍAS
11: GOLOSINAS
12: AUTOCONSUMO
13: OTROS
14: PUESTOS DE MERCADO
16: MAYORISTA ABARROTES
17: MAYORISTA ESPECIALIZ
999: NO VIAJA A DISTRIB.
NULL: NULL';
        comment on column stg.{self.schema}_{self.table}.type is 'Boca de Salida DEX';
        comment on column stg.{self.schema}_{self.table}.ubigeo_id is 'Tipo de Ubigeo Cod =>
A: ZONA ALEJADA
P: PERIFERIA
R: R
U: URBANO
V: VIAJERO
NULL: NULL';
        comment on column stg.{self.schema}_{self.table}.ubigeo is 'Tipo de Ubigeo';
        comment on column stg.{self.schema}_{self.table}.city_id is 'Ciudad Cod';
        comment on column stg.{self.schema}_{self.table}.city is 'Ciudad';
        comment on column stg.{self.schema}_{self.table}.department_id is 'Departamento Cod';
        comment on column stg.{self.schema}_{self.table}.department is 'Departamento DEX';
        comment on column stg.{self.schema}_{self.table}.province_id is 'Provincia Cod';
        comment on column stg.{self.schema}_{self.table}.province is 'Provincia DEX';
        comment on column stg.{self.schema}_{self.table}.district_id is 'Distrito DEX Cod';
        comment on column stg.{self.schema}_{self.table}.district is 'Distrito DEX';
        comment on column stg.{self.schema}_{self.table}.channel_id is 'Canal DEX Cod =>
20: MAYORISTAS
30: MINORISTAS
32: FARMACIAS TRADICIONA
33: AUTOSERVICIOS
34: CADENAS DE FARMACIAS
35: DIRECTO GASTRONOMÍA
40: PANADERÍAS
45: INDUSTRIAS
50: SUPERMERCADOS
55: INSTITUCIONES
80: DIST.MINORISTA
85: CODIST.MINORISTA
90: VARIOS
NULL: NULL';
        comment on column stg.{self.schema}_{self.table}.channel is 'Canal DEX';
        comment on column stg.{self.schema}_{self.table}.line_id is 'Giro del Cliente Cod (Giro del Cliente DEX Cod)';
        comment on column stg.{self.schema}_{self.table}.line is 'Giro del Cliente (Giro del Cliente DEX)';
        comment on column stg.{self.schema}_{self.table}.segment_id is 'Segmento DEX Cod:
A: A
A+: A+
B: B
C: C
D: D
NULL: NULL';
        comment on column stg.{self.schema}_{self.table}.segment is 'Segmento DEX';
        comment on column stg.{self.schema}_{self.table}.module_id is 'Modulo Cliente DEX Cod';
        comment on column stg.{self.schema}_{self.table}.module is 'Modulo Cliente DEX';
        comment on column stg.{self.schema}_{self.table}.classification_id is 'Clasificación de Clientes (Análisis ABC) Cod';
        comment on column stg.{self.schema}_{self.table}.classification is 'Clasificación de Clientes (Análisis ABC)';
        comment on column stg.{self.schema}_{self.table}.payment_condition_id is 'Condición de Pago Cod';
        comment on column stg.{self.schema}_{self.table}.payment_condition is 'Condición de Pago';
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
          "Cliente DEX Cod"::bigint as id,
          upper(trim("Cliente DEX")) as name,
          "Boca de Salida DEX Cod"::smallint as type_id,
          upper(trim("Boca de Salida DEX")) as type,
          upper(trim("Tipo de Ubigeo Cod")) as ubigeo_id,
          upper(trim("Tipo de Ubigeo")) as ubigeo,
          upper(trim("Ciudad Cod")) as city_id,
          upper(trim("Ciudad")) as city,
          upper(trim("Departamento Cod")) as department_id,
          upper(trim("Departamento DEX")) as department,
          upper(trim("Provincia Cod")) as province_id,
          upper(trim("Provincia DEX")) as province,
          upper(trim("Distrito DEX Cod")) as district_id,
          upper(trim("Distrito DEX")) as district,
          "Canal DEX Cod"::smallint as channel_id,
          upper(trim("Canal DEX")) as channel,
          upper(trim("Giro del Cliente Cod")) as line_dex_id,
          upper(trim("Giro del Cliente")) as line_dex,
          upper(trim("Segmento DEX Cod")) as segment_id,
          upper(trim("Segmento DEX")) as segment,
          upper(trim("Modulo Cliente DEX Cod")) as module_id,
          upper(trim("Modulo Cliente DEX")) as module,
          upper(trim("Clasificación de Clientes (Análisis ABC) Cod")) as classification_id,
          upper(trim("Clasificación de Clientes (Análisis ABC)")) as classification,
          upper(trim("Condición de Pago Cod")) as payment_condition_id,
          upper(trim("Condición de Pago")) as payment_condition
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

        comment on table {self.schema}.{self.table} is 'Customers DEX Table: [lib://QVD''s Aux Alicorp/ZQV_ZDEXCLIE.QVD]';

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

        do
        $$begin
        if not exists(
          select indexname
          from pg_catalog.pg_indexes
          where schemaname='{self.schema}' and
            tablename like '{self.table}%'
          limit 1
        ) then
          create index idx_{self.schema}_{self.table}_1 on {self.schema}.{self.table} using btree(type);
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
      self._cursor.execute(f'truncate {self.schema}.{self.table}; truncate prd.dm_customers;')
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

        insert into prd.dm_customers
        select id,name,type_id,type,ubigeo_id,ubigeo,city,department,province,
        district,channel_id,channel,line_id,line,segment_id,segment,module_id,module
        from stg.{self.schema}_{self.table};
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e