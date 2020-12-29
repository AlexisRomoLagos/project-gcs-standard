from . import Database

class MuCustomersCommercial(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'mu_customers_commercial'

  def create_raw(self):
    try:
      sql = f'''
        drop table if exists raw.{self.schema}_{self.table};
        create unlogged table raw.{self.schema}_{self.table}(
          "Número de Cliente (vista comercial) Cod" varchar(4000) null,
          "Cliente Actual Cod" varchar(4000) null,
          "Territorio Alicorp Cod" varchar(4000) null,
          "Distrito Actual Cod" varchar(4000) null,
          "Giro Actual Cod" varchar(4000) null,
          "Clasificación Cliente Actual Cod" varchar(4000) null,
          "Clasificación Cliente Negocio Cod" varchar(4000) null,
          "Nivel de Desarrollo Cod" varchar(4000) null,
          "Segmento Cod" varchar(4000) null,
          "Ger. Regional Actual cod" varchar(4000) null,
          "Oficina de ventas Actual cod" varchar(4000) null,
          "Región actual cod" varchar(4000) null,
          "Ger. Zona Actual cod" varchar(4000) null,
          "Grupo de clientes cod" varchar(4000) null,
          "Grupo de vendedores cod" varchar(4000) null,
          "Número de Cliente (vista comercial)" varchar(4000) null,
          "Cliente Actual" varchar(4000) null,
          "Territorio Alicorp" varchar(4000) null,
          "Distrito Actual" varchar(4000) null,
          "Departamento cod" varchar(4000) null,
          "Departamento" varchar(4000) null,
          "Provincia cod" varchar(4000) null,
          "Provincia" varchar(4000) null,
          "Giro Actual" varchar(4000) null,
          "Clasificación Cliente Actual" varchar(4000) null,
          "Clasificación Cliente Negocio" varchar(4000) null,
          "Nivel de Desarrollo" varchar(4000) null,
          "Segmento" varchar(4000) null,
          "Ger. Regional Actual" varchar(4000) null,
          "Oficina de ventas Actual" varchar(4000) null,
          "Región actual" varchar(4000) null,
          "Ger. Zona Actual" varchar(4000) null,
          "Grupo de clientes" varchar(4000) null,
          "Grupo de vendedores" varchar(4000) null
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
          id varchar(30) not null,--Número de Cliente (vista comercial) Cod
          name varchar(100) not null,--Número de Cliente (vista comercial)
          current_customer_id varchar(10) null,--Cliente Actual Cod
          current_customer varchar(100) not null,--Cliente Actual
          line_id varchar(2) null,--Giro Actual Cod
          line varchar(50) not null,--Giro Actual
          territory_id int null,--Territorio Alicorp Cod
          territory varchar(100) not null,--Territorio Alicorp
          region_id smallint null,--Región actual cod
          region varchar(30) not null,--Región actual
          office_id smallint null,--Oficina de ventas Actual cod
          office varchar(30) null,--Oficina de ventas Actual
          department_id varchar(6) null,--Departamento cod
          department varchar(100) null,--Departamento
          province_id varchar(6) null,--Provincia cod
          province varchar(100) null,--Provincia
          district_id varchar(6) null,--Distrito Actual Cod
          district varchar(100) null,--Distrito Actual
          customer_classification_id varchar(2) null,--Clasificación Cliente Actual Cod
          customer_classification varchar(100) null,--Clasificación Cliente Actual
          business_classification_id varchar(2) null,--Clasificación Cliente Negocio Cod
          business_classification varchar(100) null,--Clasificación Cliente Negocio
          development_level_id char(3) null,--Nivel de Desarrollo Cod
          development_level varchar(100) null,--Nivel de Desarrollo
          segment_id smallint null,--Segmento Cod
          segment varchar(100) null,--Segmento
          regional_management_id smallint null,--Ger. Regional Actual cod
          regional_management varchar(100) null,--Ger. Regional Actual
          zone_id char(3) null,--Ger. Zona Actual cod
          zone varchar(100) null,--Ger. Zona Actual
          customer_group_id char(3) null,--Grupo de clientes cod
          customer_group varchar(100) null,--Grupo de clientes
          seller_group_id smallint null,--Grupo de vendedores cod
          seller_group varchar(100) null--Grupo de vendedores
        )
        with(
          OIDS = false
        )
        tablespace pg_default;

        comment on table stg.{self.schema}_{self.table} is 'Customers Commercial Table';
        comment on column stg.{self.schema}_{self.table}.id is 'Número de Cliente (vista comercial) Cod';
        comment on column stg.{self.schema}_{self.table}.name is 'Número de Cliente (vista comercial)';
        comment on column stg.{self.schema}_{self.table}.current_customer_id is 'Cliente Actual Cod';
        comment on column stg.{self.schema}_{self.table}.current_customer is 'Cliente Actual';
        comment on column stg.{self.schema}_{self.table}.line_id is 'Giro Actual Cod';
        comment on column stg.{self.schema}_{self.table}.line is 'Giro Actual';
        comment on column stg.{self.schema}_{self.table}.territory_id is 'Territorio Alicorp Cod';
        comment on column stg.{self.schema}_{self.table}.territory is 'Territorio Alicorp';
        comment on column stg.{self.schema}_{self.table}.region_id is 'Región actual cod';
        comment on column stg.{self.schema}_{self.table}.region is 'Región actual';
        comment on column stg.{self.schema}_{self.table}.office_id is 'Oficina de ventas Actual cod';
        comment on column stg.{self.schema}_{self.table}.office is 'Oficina de ventas Actual';
        comment on column stg.{self.schema}_{self.table}.department_id is 'Departamento cod';
        comment on column stg.{self.schema}_{self.table}.department is 'Departamento';
        comment on column stg.{self.schema}_{self.table}.province_id is 'Provincia cod';
        comment on column stg.{self.schema}_{self.table}.province is 'Provincia';
        comment on column stg.{self.schema}_{self.table}.district_id is 'Distrito Actual Cod';
        comment on column stg.{self.schema}_{self.table}.district is 'Distrito Actual';
        comment on column stg.{self.schema}_{self.table}.customer_classification_id is 'Clasificación Cliente Actual Cod';
        comment on column stg.{self.schema}_{self.table}.customer_classification is 'Clasificación Cliente Actual';
        comment on column stg.{self.schema}_{self.table}.business_classification_id is 'Clasificación Cliente Negocio Cod';
        comment on column stg.{self.schema}_{self.table}.business_classification is 'Clasificación Cliente Negocio';
        comment on column stg.{self.schema}_{self.table}.development_level_id is 'Nivel de Desarrollo Cod';
        comment on column stg.{self.schema}_{self.table}.development_level is 'Nivel de Desarrollo';
        comment on column stg.{self.schema}_{self.table}.segment_id is 'Segmento Cod';
        comment on column stg.{self.schema}_{self.table}.segment is 'Segmento';
        comment on column stg.{self.schema}_{self.table}.regional_management_id is 'Ger. Regional Actual cod';
        comment on column stg.{self.schema}_{self.table}.regional_management is 'Ger. Regional Actual';
        comment on column stg.{self.schema}_{self.table}.zone_id is 'Ger. Zona Actual cod';
        comment on column stg.{self.schema}_{self.table}.zone is 'Ger. Zona Actual';
        comment on column stg.{self.schema}_{self.table}.customer_group_id is 'Grupo de clientes cod';
        comment on column stg.{self.schema}_{self.table}.customer_group is 'Grupo de clientes';
        comment on column stg.{self.schema}_{self.table}.seller_group_id is 'Grupo de vendedores cod';
        comment on column stg.{self.schema}_{self.table}.seller_group is 'Grupo de vendedores';
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
          upper(trim("Número de Cliente (vista comercial) Cod")) as id,
          upper(trim("Número de Cliente (vista comercial)")) as name,
          upper(trim("Cliente Actual Cod")) as current_customer_id,
          upper(trim("Cliente Actual")) as current_customer,
          upper(trim("Giro Actual Cod")) as line_id,
          upper(trim("Giro Actual")) as line,
          "Territorio Alicorp Cod"::int as territory_id,
          upper(trim("Territorio Alicorp")) as territory,
          "Región actual cod"::smallint as region_id,
          upper(trim("Región actual")) as region,
          "Oficina de ventas Actual cod"::smallint as office_id,
          upper(trim("Oficina de ventas Actual")) as office,
          upper(trim("Departamento cod")) as department_id,
          upper(trim("Departamento")) as department,
          upper(trim("Provincia cod")) as province_id,
          upper(trim("Provincia")) as province,
          upper(trim("Distrito Actual Cod")) as district_id,
          upper(trim("Distrito Actual")) as district,
          upper(trim("Clasificación Cliente Actual Cod")) as customer_classification_id,
          upper(trim("Clasificación Cliente Actual")) as customer_classification,
          upper(trim("Clasificación Cliente Negocio Cod")) as business_classification_id,
          upper(trim("Clasificación Cliente Negocio")) as business_classification,
          upper(trim("Nivel de Desarrollo Cod")) as development_level_id,
          upper(trim("Nivel de Desarrollo")) as development_level,
          "Segmento Cod"::smallint as segment_id,
          upper(trim("Segmento")) as segment,
          "Ger. Regional Actual cod"::smallint as regional_management_id,
          upper(trim("Ger. Regional Actual")) as regional_management,
          upper(trim("Ger. Zona Actual cod")) as zone_id,
          upper(trim("Ger. Zona Actual")) as zone,
          upper(trim("Grupo de clientes cod")) as customer_group_id,
          upper(trim("Grupo de clientes")) as customer_group,
          "Grupo de vendedores cod"::smallint as seller_group_id,
          upper(trim("Grupo de vendedores")) as seller_group
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

        comment on table {self.schema}.{self.table} is 'Customers Commercial Table: [lib://QVD''s Aux Alicorp/ZQV_IO_0CUST_SALES.QVD]';

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
      self._cursor.execute(f'truncate {self.schema}.{self.table}; truncate prd.dm_dexs;')
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

        insert into prd.dm_dexs
        select id,name,current_customer_id,current_customer,line_id,line,territory_id,territory,
        region_id,region,office_id,office,department,province,district
        from stg.{self.schema}_{self.table};
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e