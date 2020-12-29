from . import Database

class MuProducts(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'mu_products'

  def create_raw(self):
    try:
      sql = f'''
        drop table if exists raw.{self.schema}_{self.table};
        create unlogged table raw.{self.schema}_{self.table}(
          "Material Cod" varchar(4000) null,
          "Proveedor Cod" varchar(4000) null,
          "Categoria Actual Cod" varchar(4000) null,
          "Marca Actual Cod" varchar(4000) null,
          "Familia Actual Cod" varchar(4000) null,
          "Variedad Actual Cod" varchar(4000) null,
          "Linea Actual Cod" varchar(4000) null,
          "Material Actual Cod" varchar(4000) null,
          "TIER Cod" varchar(4000) null,
          "Segmento Actual Cod" varchar(4000) null,
          "Negocio Cod" varchar(4000) null,
          "Tipo Material Cod" varchar(4000) null,
          "Proveedor" varchar(4000) null,
          "Categoria Actual" varchar(4000) null,
          "Marca Actual" varchar(4000) null,
          "Familia Actual" varchar(4000) null,
          "Variedad Actual" varchar(4000) null,
          "Linea Actual" varchar(4000) null,
          "Material Actual" varchar(4000) null,
          "TIER" varchar(4000) null,
          "Segmento Actual" varchar(4000) null,
          "Negocio" varchar(4000) null,
          "Tipo Material" varchar(4000) null
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
          id varchar(20) not null,--Material Cod
          current_id varchar(20) null,--Material Actual Cod
          name varchar(100) null,--Material Actual
          brand_id char(3) null,--Marca Actual Cod
          brand varchar(100) null,--Marca Actual
          business_id char(2) null,--Negocio Cod
          business varchar(100) null,--Negocio
          category_id char(3) null,--Categoria Actual Cod
          category varchar(100) null,--Categoria Actual
          family_id char(3) null,--Familia Actual Cod
          family varchar(100) null,--Familia Actual
          line_id varchar(3) null,--Linea Actual Cod
          line varchar(100) null,--Linea Actual
          provider_id bigint null,--Proveedor Cod
          provider varchar(100) null,--Proveedor
          segment_id char(2) null,--Segmento Actual Cod
          segment varchar(100) null,--Segmento Actual
          tier_id char(2) null,--TIER Cod
          tier varchar(100) not null,--TIER
          type_id varchar(4) not null,--Tipo Material Cod
          type varchar(100) not null,--Tipo Material
          variety_id char(3) null,--Variedad Actual Cod
          variety varchar(100) null--Variedad Actual
        ) with(
          OIDS = false
        )
        tablespace pg_default;

        comment on table stg.{self.schema}_{self.table} is 'Products Table';
        comment on column stg.{self.schema}_{self.table}.id is 'Material Cod';
        comment on column stg.{self.schema}_{self.table}.current_id is 'Material Actual Cod';
        comment on column stg.{self.schema}_{self.table}.name is 'Material Actual';
        comment on column stg.{self.schema}_{self.table}.brand_id is 'Marca Actual Cod';
        comment on column stg.{self.schema}_{self.table}.brand is 'Marca Actual';
        comment on column stg.{self.schema}_{self.table}.business_id is 'Negocio Cod';
        comment on column stg.{self.schema}_{self.table}.business is 'Negocio';
        comment on column stg.{self.schema}_{self.table}.category_id is 'Categoria Actual Cod';
        comment on column stg.{self.schema}_{self.table}.category is 'Categoria Actual';
        comment on column stg.{self.schema}_{self.table}.family_id is 'Familia Actual Cod';
        comment on column stg.{self.schema}_{self.table}.family is 'Familia Actual';
        comment on column stg.{self.schema}_{self.table}.line_id is 'Linea Actual Cod';
        comment on column stg.{self.schema}_{self.table}.line is 'Linea Actual';
        comment on column stg.{self.schema}_{self.table}.provider_id is 'Proveedor Cod';
        comment on column stg.{self.schema}_{self.table}.provider is 'Proveedor';
        comment on column stg.{self.schema}_{self.table}.segment_id is 'Segmento Actual Cod';
        comment on column stg.{self.schema}_{self.table}.segment is 'Segmento Actual';
        comment on column stg.{self.schema}_{self.table}.tier_id is 'TIER Cod';
        comment on column stg.{self.schema}_{self.table}.tier is 'TIER';
        comment on column stg.{self.schema}_{self.table}.type_id is 'Tipo Material Cod';
        comment on column stg.{self.schema}_{self.table}.type is 'Tipo Material =>
FERT: PRODUCTO TERMINADO
HALB: PRODUCTO SEMITERMINADO
HAWA: MERCADERÍA
ROH: MATERIA PRIMA';
        comment on column stg.{self.schema}_{self.table}.variety_id is 'Variedad Actual Cod';
        comment on column stg.{self.schema}_{self.table}.variety is 'Variedad Actual';
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
          upper(trim("Material Cod")) as id,
          max(upper(trim("Material Actual Cod"))) as current_id,
          max((case upper(trim("Material Actual"))
            when 'SIN ASIGNAR' then null
            else upper(trim("Material Actual"))
          end)) as name,
          max((case trim("Marca Actual Cod")
            when '' then null
            else upper(trim("Marca Actual Cod"))
          end)) as brand_id,
          max((case upper(trim("Marca Actual"))
            when '' then null
            when 'SIN ASIGNAR' then null
            else upper(trim("Marca Actual"))
          end)) as brand,
          max((case trim("Negocio Cod")
            when '' then null
            else upper(trim("Negocio Cod"))
          end)) as business_id,
          max((case upper(trim("Negocio"))
            when 'SIN ASIGNAR' then null
            else upper(trim("Negocio"))
          end)) as business,
          max((case trim("Categoria Actual Cod")
            when '' then null
            else upper(trim("Categoria Actual Cod"))
          end)) as category_id,
          max((case trim("Categoria Actual")
            when '' then null
            else upper(trim("Categoria Actual"))
          end)) as category,
          max((case trim("Familia Actual Cod")
            when '' then null
            else upper(trim("Familia Actual Cod"))
          end)) as family_id,
          max((case upper(trim("Familia Actual"))
            when '' then null
            when 'SIN ASIGNAR' then null
            else upper(trim("Familia Actual"))
          end)) as family,
          max((case trim("Linea Actual Cod")
            when '' then null
            else upper(trim("Linea Actual Cod"))
          end)) as line_id,
          max((case upper(trim("Linea Actual"))
            when 'SIN ASIGNAR' then null
            else upper(trim("Linea Actual"))
          end)) as line,
          max((case trim("Proveedor Cod")
            when '' then null
            else upper(trim("Proveedor Cod"))
          end)::bigint) as provider_id,
          max((case upper(trim("Proveedor"))
            when 'SIN ASIGNAR' then null
            else upper(trim("Proveedor"))
          end)) as provider,
          max((case trim("Segmento Actual Cod")
            when '' then null
            else upper(trim("Segmento Actual Cod"))
          end)) as segment_id,
          max((case upper(trim("Segmento Actual"))
            when 'SIN ASIGNAR' then null
            else upper(trim("Segmento Actual"))
          end)) as segment,
          max((case trim("TIER Cod")
            when '' then null
            else upper(trim("TIER Cod"))
          end)) as tier_id,
          max((case upper(trim("TIER"))
            when 'SIN ASIGNAR' then '-'
            else upper(trim("TIER"))
          end)) as tier,
          max(upper(trim("Tipo Material Cod"))) as type_id,
          max(upper(trim("Tipo Material"))) as type,
          max((case trim("Variedad Actual Cod")
            when '' then null
            else upper(trim("Variedad Actual Cod"))
          end)) as variety_id,
          max((case upper(trim("Variedad Actual"))
            when 'SIN ASIGNAR' then null
            else upper(trim("Variedad Actual"))
          end)) as variety
        from
          raw.{self.schema}_{self.table}
        group by 1;
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

        comment on table {self.schema}.{self.table} is 'Products Table: [lib://QVD''s Aux Alicorp/ZQV_IO_0MATERIAL.QVD]';

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
      self._cursor.execute(f'truncate {self.schema}.{self.table}; truncate prd.dm_products;')
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

        insert into prd.dm_products
        select *
        from stg.{self.schema}_{self.table};
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e