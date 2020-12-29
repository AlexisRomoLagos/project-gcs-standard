from . import Database

class HdSalesDex(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'hd_sales_dex'

  def create_raw(self):
    try:
      sql = f'''
        drop table if exists raw.{self.schema}_{self.table};
        create unlogged table raw.{self.schema}_{self.table}(
          "Cliente DEX Cod" varchar(4000) null,
          "Negocio" varchar(4000) null,
          "Gerencia Regional Cod" varchar(4000) null,
          "Región Cod" varchar(4000) null,
          "Zona de Ventas Cod" varchar(4000) null,
          "Número de Cliente (vista comercial) Cod" varchar(4000) null,
          "Canal de Distribución Cod" varchar(4000) null,
          "Negocio Cod" varchar(4000) null,
          "Tipo de Posición de Documento Comercial Cod" varchar(4000) null,
          "Material Cod" varchar(4000) null,
          "Organización de Ventas Cod" varchar(4000) null,
          "Grupo de Vendedores Cod" varchar(4000) null,
          "Oficina de Ventas Cod" varchar(4000) null,
          "Tipo Valor para Informes Cod" varchar(4000) null,
          "Segmento Cod" varchar(4000) null,
          "Fuente Cod" varchar(4000) null,
          "Personal Vendedor DEX Cod" varchar(4000) null,
          "Tipo de Venta Telefónica DEX Cod" varchar(4000) null,
          "Territorio DEX Cod" varchar(4000) null,
          "Giro del Cliente Cod" varchar(4000) null,
          "Territorio Cod" varchar(4000) null,
          "Día Natural" varchar(4000) null,
          "Ejercicio/Período Cod" varchar(4000) null,
          "Gerencia Regional" varchar(4000) null,
          "Región" varchar(4000) null,
          "Zona de Ventas" varchar(4000) null,
          "Número de Cliente (vista comercial)" varchar(4000) null,
          "Canal de Distribución" varchar(4000) null,
          "Material" varchar(4000) null,
          "Organización de Ventas" varchar(4000) null,
          "Grupo de Vendedores" varchar(4000) null,
          "Oficina de Ventas" varchar(4000) null,
          "Tipo Valor para Informes" varchar(4000) null,
          "Cliente DEX" varchar(4000) null,
          "Segmento" varchar(4000) null,
          "Fuente" varchar(4000) null,
          "Personal Vendedor DEX" varchar(4000) null,
          "Tipo de Venta Telefónica DEX" varchar(4000) null,
          "Giro del Cliente" varchar(4000) null,
          "Territorio" varchar(4000) null,
          "Año/Semana Natural" varchar(4000) null,
          "Ejercicio/Período" varchar(4000) null,
          "Ventas Reales (S/)" varchar(4000) null,
          "Ventas Real Bruto (S/)" varchar(4000) null,
          "Ventas Plan (S/)" varchar(4000) null,
          "Peso Real (Ton)" varchar(4000) null,
          "Peso Plan (Ton)" varchar(4000) null,
          "Real (Ctd)" varchar(4000) null,
          "Plan (Ctd)" varchar(4000) null,
          "Negocio Nivel 2" varchar(4000) null,
          "Negocio Nivel 1" varchar(4000) null,
          "Impuesto" varchar(4000) null,
          "Proyección (Ctd)" varchar(4000) null,
          "Proyección (S/)" varchar(4000) null,
          "Proyección (Ton)" varchar(4000) null,
          "Proy Lineal (S/)" varchar(4000) null,
          "Proy Lineal (Ton)" varchar(4000) null,
          "Proy Lineal (Ctd)" varchar(4000) null,
          "Ventas Reales con IGV (S/)" varchar(4000) null,
          "Ventas Plan con IGV (S/)" varchar(4000) null,
          "Proy Lineal con IGV (S/)" varchar(4000) null,
          "Día" varchar(4000) null,
          "Año" varchar(4000) null,
          "Des_Mes" varchar(4000) null,
          "Cod_Mes" varchar(4000) null,
          "Periodo" varchar(4000) null,
          "Negocio_IC" varchar(4000) null,
          "Plataforma" varchar(4000) null,
          "Flag Pedido Web Cod" varchar(4000) null,
          "Flag Pedido Web" varchar(4000) null
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

  def load_raw(self,binary_file,columns):
    try:
      sql = f'''
        COPY raw.{self.schema}_{self.table}({columns})
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
          file_name char(13) not null,
          date date not null,--Día Natural

          business_id smallint not null,--Negocio Cod
          line_id varchar(2) null,--Giro del Cliente Cod
          customer_id bigint not null,--Cliente DEX Cod
          dex_id varchar(30) not null,--Número de Cliente (vista comercial) Cod
          distribution_channel_id smallint not null,--Canal de Distribución Cod
          position_type_id char(3) not null,--Tipo de Posición de Documento Comercial Cod
          product_id varchar(20) not null,--Material Cod
          product varchar(100) not null,--Material
          region_id smallint null,--Región Cod
          regional_management_id smallint null,--Gerencia Regional Cod
          office_id smallint null,--Oficina de Ventas Cod
          organization_id smallint not null,--Organización de Ventas Cod
          zone_id smallint null,--Zona de Ventas Cod
          segment_id varchar(2) null,--Segmento Cod
          seller_group_id smallint null,--Grupo de Vendedores Cod
          seller_id bigint null,--Personal Vendedor DEX Cod
          seller varchar(50) null,--Personal Vendedor DEX
          territory_dex_id int null,--Territorio DEX Cod
          territory_id smallint null,--Territorio Cod
          type_value_id smallint not null,--Tipo Valor para Informes Cod

          business_ic varchar(50) null,--Negocio_IC
          business_level_1 varchar(50) not null,--Negocio Nivel 1
          business_level_2 varchar(50) not null,--Negocio Nivel 2
          platform varchar(50) null,--Plataforma
          type varchar(50) not null,--Tipo de Venta Telefónica DEX
          source varchar(50) not null,--Fuente

          quantity_plan numeric(18,6) null,--Plan (Ctd)
          weight_plan numeric(18,6) null,--Peso Plan (Ton)
          amount_subtotal_plan numeric(18,6) null,--Ventas Plan (S/)
          amount_total_plan numeric(18,6) null,--Ventas Plan con IGV (S/)

          quantity_projection numeric(18,6) null,--Proyección (Ctd)
          weight_projection numeric(18,6) null,--Proyección (Ton)
          amount_subtotal_projection numeric(18,6) null,--Proyección (S/)

          quantity_projection_linear numeric(18,6) null,--Proy Lineal (Ctd)
          weight_projection_linear numeric(18,6) null,--Proy Lineal (Ton)
          amount_subtotal_projection_linear numeric(18,6) null,--Proy Lineal (S/)
          amount_total_projection_linear numeric(18,6) null,--Proy Lineal con IGV (S/)

          quantity numeric(18,6) not null,--Real (Ctd)
          weight numeric(18,6) not null,--Peso Real (Ton)
          amount_gross numeric(18,6) not null,--Ventas Real Bruto (S/)
          amount_subtotal numeric(18,6) not null,--Ventas Reales (S/)
          amount_tax numeric(18,6) not null,--Impuesto
          amount_total numeric(18,6) not null,--Ventas Reales con IGV (S/)

          order_web_id varchar(20) null,--Flag Pedido Web Cod
          order_web varchar(20) null--Flag Pedido Web
        )
        with(
          OIDS = false
        )
        tablespace pg_default;

        comment on table stg.{self.schema}_{self.table} is 'Sales DEX Table';
        comment on column stg.{self.schema}_{self.table}.date is 'Día Natural (Fecha Emisión)

TARGET: qlik.hw_products_sales.date';
        comment on column stg.{self.schema}_{self.table}.business_id is 'Negocio Cod =>
11: CM IMPULSO PROPIOS    (business_level_2: C.MASIVO-IMPULSO      platform: IMPULSO)
12: CM IMPULSO SOC.COM    (business_level_2: C.MASIVO-IMPULSO      platform: SSCC)
21: CM PROPIOS            (business_level_2: C.MASIVO - ABARROTES  platform: ABARROTES)
22: CM SOCIOS COMERCIALE  (business_level_2: C.MASIVO - ABARROTES  platform: SSCC)
31: NPI PROPIOS           (business_level_2: AS-PANIFICACIÓN       platform: PANIFICACIÓN)
32: NPI SOCIOS COMERCIAL  (business_level_2: AS-PANIFICACIÓN       platform: SSCC - AS)
51: FS PROPIOS            (business_level_2: AS-GASTRONOMIA        platform: FS)
52: FS SOCIOS COMERCIALE  (business_level_2: AS-GASTRONOMIA        platform: NULL)
61: GGII PROPIOS          (business_level_2: AS-INDUSTRIAS         platform: INDUSTRIAS)

DUPLICATE: qlik.mu_products.business_id

GLOSARIO:
- AS: ALICORP SOLUCIONES
- CM: CONSUMO MASIVO
- FS: FOOD SERVICES
- GGII: FLOUR INDUSTRIAL (not sure)
- NPI: NEGOCIOS DE PANIFICADORAS INDUSTRIALES
- SSCC: SOCIOS COMERCIALES';
        comment on column stg.{self.schema}_{self.table}.line_id is 'Giro del Cliente Cod =>
23: RETAIL
24: PORTUARIAS
45: RED  DEX
PR: PANIF.IND.REPARTO';
        comment on column stg.{self.schema}_{self.table}.customer_id is 'Cliente DEX Cod';
        comment on column stg.{self.schema}_{self.table}.dex_id is 'Número de Cliente (vista comercial) Cod:
Tipo Valor para Informes Cod +
Canal de Distribución Cod +
Organización de Ventas Cod +
Código';
        comment on column stg.{self.schema}_{self.table}.distribution_channel_id is 'Canal de Distribución Cod =>
20: MAYORISTAS
21: DIST.MAYORISTAS
23: CODIST.MAYORISTA
24: CONVENIENCE
30: MINORISTAS
32: FARMACIAS TRADICIONA
33: AUTOSERVICIOS
34: CADENAS DE FARMACIAS
35: DIRECTO GASTRONOMÍA
40: PANADERÍAS
41: HORECA
42: TIENDAS CONVENIENCIA
43: PANIFICADORAS INDUST
45: INDUSTRIAS
50: SUPERMERCADOS
55: INSTITUCIONES
60: FERRETERÍAS
70: HOME CENTER
75: CASH & CARRIER
80: DIST.MINORISTA
85: CODIST.MINORISTA
90: VARIOS';
        comment on column stg.{self.schema}_{self.table}.position_type_id is 'Tipo de Posición de Documento Comercial Cod =>
TBO: BONIFICACIÓN
TPR: DESCUENTO
TTG: TRANSFERENCIA GRATUITA
TNR: NORMAL DE VENTA
TVA: VALOR

TDB: DEVOLUCIÓN BONIFICACIÓN (TBO)
TDD: DEVOLUCIÓN DESCUENTO (TPR)
TDG: DEVOLUCIÓN TRANSFERENCIA GRATUITA (TTG)
TDN: DEVOLUCIÓN NORMAL DE VENTA (TNR)';
        comment on column stg.{self.schema}_{self.table}.product_id is 'Material Cod';
        comment on column stg.{self.schema}_{self.table}.product is 'Material

TARGET: qlik.hw_products_sales.name';
        comment on column stg.{self.schema}_{self.table}.region_id is 'Región Cod =>
11: LIMA
21: NORTE
22: SUR
23: ORIENTE
24: CENTRO
25: NORTE/SUR CHICO';
        comment on column stg.{self.schema}_{self.table}.regional_management_id is 'Gerencia Regional Cod =>
1: GER.REG.LIMA-N/S CH.
2: GER.REG.NORTE-ORIENT
3: GER.REG.SUR Y CENTRO
12: AS GR. PROVINCIAS
14: AS GR. LIMA
80: GER.REG.VALUE';
        comment on column stg.{self.schema}_{self.table}.office_id is 'Oficina de Ventas Cod =>
105: TUMBES
110: PIURA
120: CHICLAYO
125: CAJAMARCA
130: TRUJILLO
140: CHIMBOTE
210: HUARAZ
220: HUACHO
230: CHINCHA
240: ICA
250: AYACUCHO
310: HUÁNUCO
320: HUANCAYO
330: TARMA
410: TARAPOTO
420: PUCALLPA
440: AMAZONAS
510: AREQUIPA
520: JULIACA
530: TACNA
540: CUSCO
550: MADRE DE DIOS
553: CENTRO-APURIMAC
610: LIMA';
        comment on column stg.{self.schema}_{self.table}.organization_id is 'Organización de Ventas Cod =>
7: CM IMPULSO CORE
10: CM CORE
12: PRODUC. INDUSTRIALES
70: VALUE';
        comment on column stg.{self.schema}_{self.table}.zone_id is 'Zona de Ventas Cod =>
110: ZONA NORTE 1
130: ZONA NORTE 2
135: AS - NORTE 1
137: AS - NORTE 2
200: ZONA NORTE/SUR CHICO
310: CENTRO
322: AS - CENTRO ORIENTE
420: ZONA ORIENTE
510: ZONA SUR 1
512: AS - SUR
540: ZONA SUR 2
610: GER.ZONA MINORISTAS
612: GER.ZONA MAYORISTAS
621: AS - LIMA
860: GZ.VALUE.LIMA
861: GZ.VALUE.NORTE
862: GZ.VALUE.SUR
863: GZ.VALUE.CENT/ORIE';
        comment on column stg.{self.schema}_{self.table}.segment_id is 'Segmento Cod:
#
A
A+
B
C
D';
        comment on column stg.{self.schema}_{self.table}.seller_group_id is 'Grupo de Vendedores Cod';
        comment on column stg.{self.schema}_{self.table}.seller_id is 'Personal Vendedor DEX Cod';
        comment on column stg.{self.schema}_{self.table}.seller is 'Personal Vendedor DEX';
        comment on column stg.{self.schema}_{self.table}.territory_dex_id is 'Territorio DEX Cod';
        comment on column stg.{self.schema}_{self.table}.territory_id is 'Territorio Cod';
        comment on column stg.{self.schema}_{self.table}.type_value_id is 'Tipo Valor para Informes Cod =>
10: Real';
        comment on column stg.{self.schema}_{self.table}.business_ic is 'Negocio_IC:
ALICORP SOLUCIONES
CONSUMO MASIVO';
        comment on column stg.{self.schema}_{self.table}.business_level_1 is 'Negocio Nivel 1:
ALICORP GRUPO ROMERO';
        comment on column stg.{self.schema}_{self.table}.business_level_2 is 'Negocio Nivel 2:
AS-GASTRONOMIA
AS-INDUSTRIAS
AS-PANIFICACIÓN
C.MASIVO - ABARROTES
C.MASIVO-IMPULSO';
        comment on column stg.{self.schema}_{self.table}.platform is 'Plataforma:
ABARROTES
FS
IMPULSO
INDUSTRIAS
PANIFICACIÓN
SSCC
SSCC - AS
NULL';
        comment on column stg.{self.schema}_{self.table}.type is 'Tipo de Venta Telefónica DEX =>
#: VENTA NORMAL
VC: VENTA COMPLEMENTARIA
VE: VENDEDOR';
        comment on column stg.{self.schema}_{self.table}.source is 'Fuente =>
V: VENTA REGULAR
L: LANZAMIENTOS
K: LANZAMIENTOS (VENTA REGULAR)
P: POTENCIADORES
Q: POTENCIADORES (VENTA REGULAR)
I: IMPERDONABLES
J: IMPERDONABLES (VENTA REGULAR)
S: SURTIDO IDEAL
T: SURTIDO IDEAL (VENTA REGULAR)
O: PEDIDO SUGERIDO
VENTA NORMAL: SIDEX

OTROS:
H: NITRO 1.0
MUST STOCK LIST: letra S
N: NITRO 1.0
PRODUCTO SUGERIDO: letra O

XURAND:
M: NITRO 1.0
N: NITRO 1.0
X: NITRO 1.0';
        comment on column stg.{self.schema}_{self.table}.quantity_plan is 'Plan (Ctd)';
        comment on column stg.{self.schema}_{self.table}.weight_plan is 'Peso Plan (Ton)';
        comment on column stg.{self.schema}_{self.table}.amount_subtotal_plan is 'Ventas Plan (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_total_plan is 'Ventas Plan con IGV (S/)';
        comment on column stg.{self.schema}_{self.table}.quantity_projection is 'Proyección (Ctd)';
        comment on column stg.{self.schema}_{self.table}.weight_projection is 'Proyección (Ton)';
        comment on column stg.{self.schema}_{self.table}.amount_subtotal_projection is 'Proyección (S/)';
        comment on column stg.{self.schema}_{self.table}.quantity_projection_linear is 'Proy Lineal (Ctd)';
        comment on column stg.{self.schema}_{self.table}.weight_projection_linear is 'Proy Lineal (Ton)';
        comment on column stg.{self.schema}_{self.table}.amount_subtotal_projection_linear is 'Proy Lineal (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_total_projection_linear is 'Proy Lineal con IGV (S/)';
        comment on column stg.{self.schema}_{self.table}.quantity is 'Real (Ctd)';
        comment on column stg.{self.schema}_{self.table}.weight is 'Peso Real (Ton)';
        comment on column stg.{self.schema}_{self.table}.amount_gross is 'Ventas Real Bruto (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_subtotal is 'Ventas Reales (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_tax is 'Impuesto';
        comment on column stg.{self.schema}_{self.table}.amount_total is 'Ventas Reales con IGV (S/)';
        comment on column stg.{self.schema}_{self.table}.order_web_id is 'Flag Pedido Web Cod =>
A: ALIMARKET
B: MARKETPLACE B2B';
        comment on column stg.{self.schema}_{self.table}.order_web is 'Flag Pedido Web';
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e

  def transformation_stg(self,date):
    try:
      sql = f'''
        insert into stg.{self.schema}_{self.table}
        select
          ({date} || '.csv') as file_name,
          to_date("Día Natural",'DD.MM.YYYY') as date,
          "Negocio Cod"::smallint as business_id,
          upper(trim("Giro del Cliente Cod")) as line_id,
          "Cliente DEX Cod"::bigint as customer_id,
          upper(trim("Número de Cliente (vista comercial) Cod")) as dex_id,
          "Canal de Distribución Cod"::smallint as distribution_channel_id,
          upper(trim("Tipo de Posición de Documento Comercial Cod")) as position_type_id,
          upper(trim("Material Cod")) as product_id,
          upper(trim(replace("Material",chr(160),''))) as product,
          "Región Cod"::smallint as region_id,
          "Gerencia Regional Cod"::smallint as regional_management_id,
          "Oficina de Ventas Cod"::smallint as office_id,
          "Organización de Ventas Cod"::smallint as organization_id,
          "Zona de Ventas Cod"::smallint as zone_id,
          upper(trim("Segmento Cod")) as segment_id,
          "Grupo de Vendedores Cod"::smallint as seller_group_id,
          "Personal Vendedor DEX Cod"::bigint as seller_id,
          upper(trim("Personal Vendedor DEX")) as seller,
          "Territorio DEX Cod"::int as territory_dex_id,
          "Territorio Cod"::smallint as territory_id,
          "Tipo Valor para Informes Cod"::smallint as type_value_id,

          upper(trim("Negocio_IC")) as business_ic,
          upper(trim("Negocio Nivel 1")) as business_level_1,
          upper(trim("Negocio Nivel 2")) as business_level_2,
          upper(trim("Plataforma")) as platform,
          upper(trim("Tipo de Venta Telefónica DEX")) as type,
          upper(trim("Fuente")) as source,

          "Plan (Ctd)"::numeric(18,6) as quantity_plan,
          "Peso Plan (Ton)"::numeric(18,6) as weight_plan,
          "Ventas Plan (S/)"::numeric(18,6) as amount_subtotal_plan,
          "Ventas Plan con IGV (S/)"::numeric(18,6) as amount_total_plan,

          "Proyección (Ctd)"::numeric(18,6) as quantity_projection,
          "Proyección (Ton)"::numeric(18,6) as weight_projection,
          "Proyección (S/)"::numeric(18,6) as amount_subtotal_projection,

          "Proy Lineal (Ctd)"::numeric(18,6) as quantity_projection_linear,
          "Proy Lineal (Ton)"::numeric(18,6) as weight_projection_linear,
          "Proy Lineal (S/)"::numeric(18,6) as amount_subtotal_projection_linear,
          "Proy Lineal con IGV (S/)"::numeric(18,6) as amount_total_projection_linear,

          "Real (Ctd)"::numeric(18,6) as quantity,
          "Peso Real (Ton)"::numeric(18,6) as weight,
          coalesce("Ventas Real Bruto (S/)"::numeric(18,6),0) as amount_gross,
          "Ventas Reales (S/)"::numeric(18,6) as amount_subtotal,
          coalesce("Impuesto"::numeric(18,6),0) as amount_tax,
          coalesce("Ventas Reales con IGV (S/)"::numeric(18,6),0) as amount_total,

          upper(trim("Flag Pedido Web Cod")) as order_web_id,
          upper(trim("Flag Pedido Web")) as order_web
        from
          raw.{self.schema}_{self.table}
        where
          "Cliente DEX Cod" ~ '^\d+$' and
          "Día Natural" is not null
          --to_date("Día Natural",'DD.MM.YYYY') = '{date}' and
          --"Grupo de Vendedores Cod" is not null and
          --"Personal Vendedor DEX Cod" is not null and
          --"Territorio DEX Cod" is not null
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
          partition by range(date)
          tablespace pg_default;

        comment on table {self.schema}.{self.table} is 'Sales DEX Table: [lib://QVDs Alicorp (grupoalicorp_mninapi)/Análisis de Ventas DEX\ZQV_ZDEX_M001_Q01_YYYYMMDD.QVD]';

        create table if not exists {self.schema}.{self.table}_201601 partition of {self.schema}.{self.table} for values from ('2016-01-01') to ('2016-02-01');
        create table if not exists {self.schema}.{self.table}_201602 partition of {self.schema}.{self.table} for values from ('2016-02-01') to ('2016-03-01');
        create table if not exists {self.schema}.{self.table}_201603 partition of {self.schema}.{self.table} for values from ('2016-03-01') to ('2016-04-01');
        create table if not exists {self.schema}.{self.table}_201604 partition of {self.schema}.{self.table} for values from ('2016-04-01') to ('2016-05-01');
        create table if not exists {self.schema}.{self.table}_201605 partition of {self.schema}.{self.table} for values from ('2016-05-01') to ('2016-06-01');
        create table if not exists {self.schema}.{self.table}_201606 partition of {self.schema}.{self.table} for values from ('2016-06-01') to ('2016-07-01');
        create table if not exists {self.schema}.{self.table}_201607 partition of {self.schema}.{self.table} for values from ('2016-07-01') to ('2016-08-01');
        create table if not exists {self.schema}.{self.table}_201608 partition of {self.schema}.{self.table} for values from ('2016-08-01') to ('2016-09-01');
        create table if not exists {self.schema}.{self.table}_201609 partition of {self.schema}.{self.table} for values from ('2016-09-01') to ('2016-10-01');
        create table if not exists {self.schema}.{self.table}_201610 partition of {self.schema}.{self.table} for values from ('2016-10-01') to ('2016-11-01');
        create table if not exists {self.schema}.{self.table}_201611 partition of {self.schema}.{self.table} for values from ('2016-11-01') to ('2016-12-01');
        create table if not exists {self.schema}.{self.table}_201612 partition of {self.schema}.{self.table} for values from ('2016-12-01') to ('2017-01-01');

        create table if not exists {self.schema}.{self.table}_201701 partition of {self.schema}.{self.table} for values from ('2017-01-01') to ('2017-02-01');
        create table if not exists {self.schema}.{self.table}_201702 partition of {self.schema}.{self.table} for values from ('2017-02-01') to ('2017-03-01');
        create table if not exists {self.schema}.{self.table}_201703 partition of {self.schema}.{self.table} for values from ('2017-03-01') to ('2017-04-01');
        create table if not exists {self.schema}.{self.table}_201704 partition of {self.schema}.{self.table} for values from ('2017-04-01') to ('2017-05-01');
        create table if not exists {self.schema}.{self.table}_201705 partition of {self.schema}.{self.table} for values from ('2017-05-01') to ('2017-06-01');
        create table if not exists {self.schema}.{self.table}_201706 partition of {self.schema}.{self.table} for values from ('2017-06-01') to ('2017-07-01');
        create table if not exists {self.schema}.{self.table}_201707 partition of {self.schema}.{self.table} for values from ('2017-07-01') to ('2017-08-01');
        create table if not exists {self.schema}.{self.table}_201708 partition of {self.schema}.{self.table} for values from ('2017-08-01') to ('2017-09-01');
        create table if not exists {self.schema}.{self.table}_201709 partition of {self.schema}.{self.table} for values from ('2017-09-01') to ('2017-10-01');
        create table if not exists {self.schema}.{self.table}_201710 partition of {self.schema}.{self.table} for values from ('2017-10-01') to ('2017-11-01');
        create table if not exists {self.schema}.{self.table}_201711 partition of {self.schema}.{self.table} for values from ('2017-11-01') to ('2017-12-01');
        create table if not exists {self.schema}.{self.table}_201712 partition of {self.schema}.{self.table} for values from ('2017-12-01') to ('2018-01-01');

        create table if not exists {self.schema}.{self.table}_201801 partition of {self.schema}.{self.table} for values from ('2018-01-01') to ('2018-02-01');
        create table if not exists {self.schema}.{self.table}_201802 partition of {self.schema}.{self.table} for values from ('2018-02-01') to ('2018-03-01');
        create table if not exists {self.schema}.{self.table}_201803 partition of {self.schema}.{self.table} for values from ('2018-03-01') to ('2018-04-01');
        create table if not exists {self.schema}.{self.table}_201804 partition of {self.schema}.{self.table} for values from ('2018-04-01') to ('2018-05-01');
        create table if not exists {self.schema}.{self.table}_201805 partition of {self.schema}.{self.table} for values from ('2018-05-01') to ('2018-06-01');
        create table if not exists {self.schema}.{self.table}_201806 partition of {self.schema}.{self.table} for values from ('2018-06-01') to ('2018-07-01');
        create table if not exists {self.schema}.{self.table}_201807 partition of {self.schema}.{self.table} for values from ('2018-07-01') to ('2018-08-01');
        create table if not exists {self.schema}.{self.table}_201808 partition of {self.schema}.{self.table} for values from ('2018-08-01') to ('2018-09-01');
        create table if not exists {self.schema}.{self.table}_201809 partition of {self.schema}.{self.table} for values from ('2018-09-01') to ('2018-10-01');
        create table if not exists {self.schema}.{self.table}_201810 partition of {self.schema}.{self.table} for values from ('2018-10-01') to ('2018-11-01');
        create table if not exists {self.schema}.{self.table}_201811 partition of {self.schema}.{self.table} for values from ('2018-11-01') to ('2018-12-01');
        create table if not exists {self.schema}.{self.table}_201812 partition of {self.schema}.{self.table} for values from ('2018-12-01') to ('2019-01-01');

        create table if not exists {self.schema}.{self.table}_201901 partition of {self.schema}.{self.table} for values from ('2019-01-01') to ('2019-02-01');
        create table if not exists {self.schema}.{self.table}_201902 partition of {self.schema}.{self.table} for values from ('2019-02-01') to ('2019-03-01');
        create table if not exists {self.schema}.{self.table}_201903 partition of {self.schema}.{self.table} for values from ('2019-03-01') to ('2019-04-01');
        create table if not exists {self.schema}.{self.table}_201904 partition of {self.schema}.{self.table} for values from ('2019-04-01') to ('2019-05-01');
        create table if not exists {self.schema}.{self.table}_201905 partition of {self.schema}.{self.table} for values from ('2019-05-01') to ('2019-06-01');
        create table if not exists {self.schema}.{self.table}_201906 partition of {self.schema}.{self.table} for values from ('2019-06-01') to ('2019-07-01');
        create table if not exists {self.schema}.{self.table}_201907 partition of {self.schema}.{self.table} for values from ('2019-07-01') to ('2019-08-01');
        create table if not exists {self.schema}.{self.table}_201908 partition of {self.schema}.{self.table} for values from ('2019-08-01') to ('2019-09-01');
        create table if not exists {self.schema}.{self.table}_201909 partition of {self.schema}.{self.table} for values from ('2019-09-01') to ('2019-10-01');
        create table if not exists {self.schema}.{self.table}_201910 partition of {self.schema}.{self.table} for values from ('2019-10-01') to ('2019-11-01');
        create table if not exists {self.schema}.{self.table}_201911 partition of {self.schema}.{self.table} for values from ('2019-11-01') to ('2019-12-01');
        create table if not exists {self.schema}.{self.table}_201912 partition of {self.schema}.{self.table} for values from ('2019-12-01') to ('2020-01-01');

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
        if not exists(
          select indexname
          from pg_catalog.pg_indexes
          where schemaname='{self.schema}' and
            tablename like '{self.table}%'
          limit 1
        ) then
          create index idx_{self.schema}_{self.table}_1 on {self.schema}.{self.table} using btree(file_name);
          create index idx_{self.schema}_{self.table}_2 on {self.schema}.{self.table} using btree(customer_id);
          create index idx_{self.schema}_{self.table}_3 on {self.schema}.{self.table} using btree(product_id);
          create index idx_{self.schema}_{self.table}_4 on {self.schema}.{self.table} using btree(dex_id);
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
          file_name = '{date}.csv';

        delete from
          prd.dm_sales
        where
          date = '{date}';
      '''
      self._cursor.execute(sql)
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

        insert into prd.dm_sales
        select date,business_id,line_id,customer_id,dex_id,distribution_channel_id,position_type_id,
        product_id,product,region_id,regional_management_id,office_id,organization_id,zone_id,segment_id,
        seller_group_id,seller_id,seller,territory_dex_id,territory_id,type_value_id,business_ic,
        business_level_1,business_level_2,platform,quantity,weight,type,source,
        amount_gross,amount_subtotal,amount_tax,amount_total
        from stg.{self.schema}_{self.table};
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e