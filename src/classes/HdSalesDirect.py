from . import Database

class HdSalesDirect(Database):
  def __init__(self,client,cursor):
    super().__init__(client,cursor)
    self.schema = 'qlik'
    self.table = 'hd_sales_direct'

  def create_raw(self):
    try:
      sql = f'''
        drop table if exists raw.{self.schema}_{self.table};
        create unlogged table raw.{self.schema}_{self.table}(
          "Ejercicio/Período Cod" varchar(4000) null,
          "Clase de Factura Cod" varchar(4000) null,
          "Clase de Factura" varchar(4000) null,
          "Número de Cliente Cod" varchar(4000) null,
          "Número de Cliente" varchar(4000) null,
          "Clasificación Cliente Cod" varchar(4000) null,
          "Clasificación Cliente" varchar(4000) null,
          "Región de Ventas Cod" varchar(4000) null,
          "Región de Ventas" varchar(4000) null,
          "Región Geográfica Cod" varchar(4000) null,
          "Región Geográfica" varchar(4000) null,
          "Zona de Ventas Cod" varchar(4000) null,
          "Zona de Ventas" varchar(4000) null,
          "Número de Cliente (Vista Comercial) Cod" varchar(4000) null,
          "Número de Cliente (Vista Comercial)" varchar(4000) null,
          "Canal de Distribución Cod" varchar(4000) null,
          "Canal de Distribución" varchar(4000) null,
          "Número de Pedido" varchar(4000) null,
          "Clase de Pedido Cod" varchar(4000) null,
          "Clase de Pedido" varchar(4000) null,
          "Fecha de Pedido" varchar(4000) null,
          "Tipo de Posición Cod" varchar(4000) null,
          "Tipo de Posición" varchar(4000) null,
          "Material Cod" varchar(4000) null,
          "Material" varchar(4000) null,
          "Motivo de Pedido Cod" varchar(4000) null,
          "Motivo de Pedido" varchar(4000) null,
          "Centro Cod" varchar(4000) null,
          "Centro" varchar(4000) null,
          "Clave de Condiciones de Pago Cod" varchar(4000) null,
          "Clave de Condiciones de Pago" varchar(4000) null,
          "Clase de Operación Cod" varchar(4000) null,
          "Clase de Operación" varchar(4000) null,
          "Organización de Ventas Cod" varchar(4000) null,
          "Organización de Ventas" varchar(4000) null,
          "Grupo de Vendedores Cod" varchar(4000) null,
          "Grupo de Vendedores" varchar(4000) null,
          "Oficina de Ventas Cod" varchar(4000) null,
          "Oficina de Ventas" varchar(4000) null,
          "Destinatario de la Mercancía Cod" varchar(4000) null,
          "Destinatario de la Mercancía" varchar(4000) null,
          "Tipo de Valor Cod" varchar(4000) null,
          "Tipo de Valor" varchar(4000) null,
          "Tipo de Venta Cod" varchar(4000) null,
          "Tipo de Venta" varchar(4000) null,
          "Territorio Cod" varchar(4000) null,
          "Territorio" varchar(4000) null,
          "Número de Documento" varchar(4000) null,
          "Posición de Documento Cod" varchar(4000) null,
          "Posición de Documento" varchar(4000) null,
          "Estado de Pendiente Cod" varchar(4000) null,
          "Estado de Pendiente" varchar(4000) null,
          "Día Natural" varchar(4000) null,
          "Año/Semana Natural" varchar(4000) null,
          "Ejercicio/Período" varchar(4000) null,
          "Unidad de Medida Base" varchar(4000) null,
          "Moneda" varchar(4000) null,
          "UM Volumen de Ventas" varchar(4000) null,
          "Proyección en Cant." varchar(4000) null,
          "SD Peso Neto" varchar(4000) null,
          "Proyección en Moneda" varchar(4000) null,
          "Proyección en Peso" varchar(4000) null,
          "Ingreso Neto" varchar(4000) null,
          "Ingreso Bruto" varchar(4000) null,
          "Volumen Ventas" varchar(4000) null,
          "Peso Devoluciones" varchar(4000) null,
          "Cantidad Devoluciones" varchar(4000) null,
          "Importe Devoluciones" varchar(4000) null,
          "Peso Entrega Pend." varchar(4000) null,
          "Peso Pedido Pend." varchar(4000) null,
          "Cantidad Entrega Pend." varchar(4000) null,
          "Cantidad Pedido Pend." varchar(4000) null,
          "Importe Entrega Pend." varchar(4000) null,
          "Importe Pedido Pend." varchar(4000) null,
          "InfoProvider" varchar(4000) null,
          "Flag_Estado" varchar(4000) null,
          "Flag_Estado2" varchar(4000) null,
          "Días H Restantes" varchar(4000) null,
          "Días Hábiles" varchar(4000) null,
          "Real (Ton)" varchar(4000) null,
          "Plan (Ton)" varchar(4000) null,
          "Ped/Ent (Ton)" varchar(4000) null,
          "A Facturar en el Mes (Ton)" varchar(4000) null,
          "A Facturar a la Fecha (Ton)" varchar(4000) null,
          "Retenido por Crédito (Ton)" varchar(4000) null,
          "Bloqueo de Entrega (Ton)" varchar(4000) null,
          "Cliente Recog (Ton)" varchar(4000) null,
          "Con Stock Asignado (Ton)" varchar(4000) null,
          "Con Stock (Ton)" varchar(4000) null,
          "Sin Stock (Ton)" varchar(4000) null,
          "A Facturar en el Resto del Mes (Ton)" varchar(4000) null,
          "Meses Futuros (Ton)" varchar(4000) null,
          "Proy (Ton)" varchar(4000) null,
          "Real (Ctd)" varchar(4000) null,
          "Plan (Ctd)" varchar(4000) null,
          "Proy (Ctd)" varchar(4000) null,
          "Ped/Ent (Ctd)" varchar(4000) null,
          "A Facturar en el Mes (Ctd)" varchar(4000) null,
          "A Facturar a la Fecha (Ctd)" varchar(4000) null,
          "Retenido por Crédito (Ctd)" varchar(4000) null,
          "Bloqueo de Entrega (Ctd)" varchar(4000) null,
          "Cliente Recog (Ctd)" varchar(4000) null,
          "Con Stock Asignado (Ctd)" varchar(4000) null,
          "Con Stock (Ctd)" varchar(4000) null,
          "Sin Stock (Ctd)" varchar(4000) null,
          "A Facturar en el Resto del Mes (Ctd)" varchar(4000) null,
          "Meses Futuros (Ctd)" varchar(4000) null,
          "Real (S/)" varchar(4000) null,
          "Plan (S/)" varchar(4000) null,
          "Real Bruto (S/)" varchar(4000) null,
          "Proy (S/)" varchar(4000) null,
          "Ped/Ent (S/)" varchar(4000) null,
          "A Facturar en el Mes (S/)" varchar(4000) null,
          "A Facturar a la Fecha (S/)" varchar(4000) null,
          "Retenido por Crédito (S/)" varchar(4000) null,
          "Bloqueo de Entrega (S/)" varchar(4000) null,
          "Cliente Recog (S/)" varchar(4000) null,
          "Con Stock Asignado (S/)" varchar(4000) null,
          "Con Stock (S/)" varchar(4000) null,
          "Sin Stock (S/)" varchar(4000) null,
          "A Facturar en el Resto del Mes (S/)" varchar(4000) null,
          "Meses Futuros (S/)" varchar(4000) null,
          "Proy Lineal (Ton)" varchar(4000) null,
          "Proy Lineal (S/)" varchar(4000) null,
          "Oficina de Ventas Nivel 2" varchar(4000) null,
          "Oficina de Ventas Nivel 1" varchar(4000) null,
          "Periodo en Curso" varchar(4000) null,
          "Periodo Cerrado" varchar(4000) null
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
          file_name char(13) not null,
          date date not null,--Día Natural

          customer_id bigint not null,--Número de Cliente Cod
          customer_commercial_id varchar(30) null,--Número de Cliente (Vista Comercial) Cod

          order_id varchar(20) null,--Número de Pedido
          order_date date null,--Fecha de Pedido
          order_class_id varchar(4) null,--Clase de Pedido Cod
          order_reason_id char(3) null,--Motivo de Pedido Cod

          distribution_channel_id smallint null,--Canal de Distribución Cod
          position_type_id varchar(4) null,--Tipo de Posición Cod
          product_id varchar(20) null,--Material Cod
          group_id smallint null,--Grupo de Vendedores Cod
          office_id smallint null,--Oficina de Ventas Cod
          region_id smallint null,--Región de Ventas Cod
          region_geography_id smallint null,--Región Geográfica Cod
          zone_id smallint null,--Zona de Ventas Cod
          territory_id smallint null,--Territorio Cod

          invoice_type_id char(4) null,--Clase de Factura Cod
          classification_id varchar(2) null,--Clasificación Cliente Cod
          organization_id smallint null,--Organización de Ventas Cod
          recipient_id bigint null,--Destinatario de la Mercancía Cod
          value_id smallint null,--Tipo de Valor Cod
          type_id varchar(20) null,--Tipo de Venta Cod
          document_id varchar(20) null,--Número de Documento
          document_position_id varchar(20) null,--Posición de Documento Cod
          status_id varchar(20) null,--Estado de Pendiente Cod
          center_id char(4) null,--Centro Cod
          condition_id char(4) null,--Clave de Condiciones de Pago Cod
          operation_id char(1) null,--Clase de Operación Cod
          unit_id varchar(20) null,--Unidad de Medida Base
          coin varchar(20) null,--Moneda
          volume numeric(18,6) null,--Volumen Ventas
          volume_measure varchar(20) null,--UM Volumen de Ventas
          net_weight numeric(18,6) null,--SD Peso Neto
          net_income numeric(18,6) null,--Ingreso Neto
          gross_income numeric(18,6) null,--Ingreso Bruto

          provider_id char(3) null,--InfoProvider
          flag_status varchar(20) null,--Flag_Estado
          flag_status_2 varchar(20) null,--Flag_Estado2
          remain_days numeric(2) null,--Días H Restantes
          business_days numeric(2) null,--Días Hábiles
          sales_office_1 varchar(20) null,--Oficina de Ventas Nivel 1
          sales_office_2 varchar(20) null,--Oficina de Ventas Nivel 2
          period_current char(1) null,--Periodo en Curso
          period_closed char(1) null,--Periodo Cerrado

          weight_order numeric(18,6) null,--Peso Pedido Pend.
          weight_delivery numeric(18,6) null,--Peso Entrega Pend.
          weight_order_delivery numeric(18,6) null,--Ped/Ent (Ton)
          weight_check_in_month numeric(18,6) null,--A Facturar en el Mes (Ton)
          weight_check_in_month_2 numeric(18,6) null,--A Facturar en el Resto del Mes (Ton)
          weight_check_in_day numeric(18,6) null,--A Facturar a la Fecha (Ton)
          weight_detained_credit numeric(18,6) null,--Retenido por Crédito (Ton)
          weight_block_delivery numeric(18,6) null,--Bloqueo de Entrega (Ton)
          weight_customer_collect numeric(18,6) null,--Cliente Recog (Ton)
          weight_with_assigned_stock numeric(18,6) null,--Con Stock Asignado (Ton)
          weight_with_stock numeric(18,6) null,--Con Stock (Ton)
          weight_without_stock numeric(18,6) null,--Sin Stock (Ton)
          weight_future_months numeric(18,6) null,--Meses Futuros (Ton)
          weight_projection numeric(18,6) null,--Proy (Ton)
          weight_projection_2 numeric(18,6) null,--Proyección en Peso
          weight_projection_linear numeric(18,6) null,--Proy Lineal (Ton)
          weight_plan numeric(18,6) null,--Plan (Ton)
          weight_return numeric(18,6) null,--Peso Devoluciones

          quantity_order numeric(18,6) null,--Cantidad Pedido Pend.
          quantity_delivery numeric(18,6) null,--Cantidad Entrega Pend.
          quantity_order_delivery numeric(18,6) null,--Ped/Ent (Ctd)
          quantity_check_in_month numeric(18,6) null,--A Facturar en el Mes (Ctd)
          quantity_check_in_month_2 numeric(18,6) null,--A Facturar en el Resto del Mes (Ctd)
          quantity_check_in_day numeric(18,6) null,--A Facturar a la Fecha (Ctd)
          quantity_detained_credit numeric(18,6) null,--Retenido por Crédito (Ctd)
          quantity_block_delivery numeric(18,6) null,--Bloqueo de Entrega (Ctd)
          quantity_customer_collect numeric(18,6) null,--Cliente Recog (Ctd)
          quantity_with_assigned_stock numeric(18,6) null,--Con Stock Asignado (Ctd)
          quantity_with_stock numeric(18,6) null,--Con Stock (Ctd)
          quantity_without_stock numeric(18,6) null,--Sin Stock (Ctd)
          quantity_future_months numeric(18,6) null,--Meses Futuros (Ctd)
          quantity_projection numeric(18,6) null,--Proy (Ctd)
          quantity_projection_2 numeric(18,6) null,--Proyección en Cant.
          quantity_plan numeric(18,6) null,--Plan (Ctd)
          quantity_return numeric(18,6) null,--Cantidad Devoluciones

          amount_order numeric(18,6) null,--Importe Pedido Pend.
          amount_delivery numeric(18,6) null,--Importe Entrega Pend.
          amount_order_delivery numeric(18,6) null,--Ped/Ent (S/)
          amount_check_in_month numeric(18,6) null,--A Facturar en el Mes (S/)
          amount_check_in_month_2 numeric(18,6) null,--A Facturar en el Resto del Mes (S/)
          amount_check_in_day numeric(18,6) null,--A Facturar a la Fecha (S/)
          amount_detained_credit numeric(18,6) null,--Retenido por Crédito (S/)
          amount_block_delivery numeric(18,6) null,--Bloqueo de Entrega (S/)
          amount_customer_collect numeric(18,6) null,--Cliente Recog (S/)
          amount_with_assigned_stock numeric(18,6) null,--Con Stock Asignado (S/)
          amount_with_stock numeric(18,6) null,--Con Stock (S/)
          amount_without_stock numeric(18,6) null,--Sin Stock (S/)
          amount_future_months numeric(18,6) null,--Meses Futuros (S/)
          amount_projection numeric(18,6) null,--Proy (S/)
          amount_projection_2 numeric(18,6) null,--Proyección en Moneda
          amount_projection_linear numeric(18,6) null,--Proy Lineal (S/)
          amount_plan numeric(18,6) null,--Plan (S/)
          amount_return numeric(18,6) null,--Importe Devoluciones

          quantity numeric(18,6) null,--Real (Ctd)
          weight numeric(18,6) null,--Real (Ton)
          amount_gross numeric(18,6) null,--Real Bruto (S/)
          amount_subtotal numeric(18,6) null--Real (S/)
        ) with(
          OIDS = false
        )
        tablespace pg_default;

        comment on table stg.{self.schema}_{self.table} is 'Sales Direct Table';
        comment on column stg.{self.schema}_{self.table}.file_name is 'Nombre del archivo del bucket';
        comment on column stg.{self.schema}_{self.table}.date is 'Día Natural';
        comment on column stg.{self.schema}_{self.table}.customer_id is 'Número de Cliente Cod';
        comment on column stg.{self.schema}_{self.table}.customer_commercial_id is 'Número de Cliente (Vista Comercial) Cod';
        comment on column stg.{self.schema}_{self.table}.order_id is 'Número de Pedido';
        comment on column stg.{self.schema}_{self.table}.order_date is 'Fecha de Pedido';
        comment on column stg.{self.schema}_{self.table}.order_class_id is 'Clase de Pedido Cod';
        comment on column stg.{self.schema}_{self.table}.order_reason_id is 'Motivo de Pedido Cod';
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
        comment on column stg.{self.schema}_{self.table}.position_type_id is 'Tipo de Posición Cod =>
G2N: SOLICITUD
L2N: 
TAN: POSICIÓN NORMAL
TANN: POSICIÓN SIN CARGO
ZPD1: 
ZRAG: 
ZRAN: 
ZREN: 
ZROI: 
ZRTG: 
ZTOI: POSICIÓN NORMAL
ZTRG: POSICIÓN TRANSF.GRAT
ZTRH: ';
        comment on column stg.{self.schema}_{self.table}.product_id is 'Material Cod';
        comment on column stg.{self.schema}_{self.table}.group_id is 'Grupo de Vendedores Cod';
        comment on column stg.{self.schema}_{self.table}.office_id is 'Oficina de Ventas Cod';
        comment on column stg.{self.schema}_{self.table}.region_id is 'Región de Ventas Cod';
        comment on column stg.{self.schema}_{self.table}.region_geography_id is 'Región Geográfica Cod';
        comment on column stg.{self.schema}_{self.table}.zone_id is 'Zona de Ventas Cod';
        comment on column stg.{self.schema}_{self.table}.territory_id is 'Territorio Cod';
        comment on column stg.{self.schema}_{self.table}.invoice_type_id is 'Clase de Factura Cod';
        comment on column stg.{self.schema}_{self.table}.classification_id is 'Clasificación Cliente Cod';
        comment on column stg.{self.schema}_{self.table}.organization_id is 'Organización de Ventas Cod';
        comment on column stg.{self.schema}_{self.table}.recipient_id is 'Destinatario de la Mercancía Cod';
        comment on column stg.{self.schema}_{self.table}.value_id is 'Tipo de Valor Cod';
        comment on column stg.{self.schema}_{self.table}.type_id is 'Tipo de Venta Cod';
        comment on column stg.{self.schema}_{self.table}.document_id is 'Número de Documento';
        comment on column stg.{self.schema}_{self.table}.document_position_id is 'Posición de Documento Cod';
        comment on column stg.{self.schema}_{self.table}.status_id is 'Estado de Pendiente Cod';
        comment on column stg.{self.schema}_{self.table}.center_id is 'Centro Cod';
        comment on column stg.{self.schema}_{self.table}.condition_id is 'Clave de Condiciones de Pago Cod';
        comment on column stg.{self.schema}_{self.table}.operation_id is 'Clase de Operación Cod';
        comment on column stg.{self.schema}_{self.table}.unit_id is 'Unidad de Medida Base';
        comment on column stg.{self.schema}_{self.table}.coin is 'Moneda';
        comment on column stg.{self.schema}_{self.table}.volume is 'Volumen Ventas';
        comment on column stg.{self.schema}_{self.table}.volume_measure is 'UM Volumen de Ventas';
        comment on column stg.{self.schema}_{self.table}.net_weight is 'SD Peso Neto';
        comment on column stg.{self.schema}_{self.table}.net_income is 'Ingreso Neto';
        comment on column stg.{self.schema}_{self.table}.gross_income is 'Ingreso Bruto';
        comment on column stg.{self.schema}_{self.table}.provider_id is 'InfoProvider';
        comment on column stg.{self.schema}_{self.table}.flag_status is 'Flag_Estado';
        comment on column stg.{self.schema}_{self.table}.flag_status_2 is 'Flag_Estado2';
        comment on column stg.{self.schema}_{self.table}.remain_days is 'Días H Restantes';
        comment on column stg.{self.schema}_{self.table}.business_days is 'Días Hábiles';
        comment on column stg.{self.schema}_{self.table}.sales_office_1 is 'Oficina de Ventas Nivel 1';
        comment on column stg.{self.schema}_{self.table}.sales_office_2 is 'Oficina de Ventas Nivel 2';
        comment on column stg.{self.schema}_{self.table}.period_current is 'Periodo en Curso';
        comment on column stg.{self.schema}_{self.table}.period_closed is 'Periodo Cerrado';
        comment on column stg.{self.schema}_{self.table}.weight_order is 'Peso Pedido Pend.';
        comment on column stg.{self.schema}_{self.table}.weight_delivery is 'Peso Entrega Pend.';
        comment on column stg.{self.schema}_{self.table}.weight_order_delivery is 'Ped/Ent (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_check_in_month is 'A Facturar en el Mes (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_check_in_month_2 is 'A Facturar en el Resto del Mes (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_check_in_day is 'A Facturar a la Fecha (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_detained_credit is 'Retenido por Crédito (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_block_delivery is 'Bloqueo de Entrega (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_customer_collect is 'Cliente Recog (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_with_assigned_stock is 'Con Stock Asignado (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_with_stock is 'Con Stock (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_without_stock is 'Sin Stock (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_future_months is 'Meses Futuros (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_projection is 'Proy (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_projection_2 is 'Proyección en Peso';
        comment on column stg.{self.schema}_{self.table}.weight_projection_linear is 'Proy Lineal (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_plan is 'Plan (Ton)';
        comment on column stg.{self.schema}_{self.table}.weight_return is 'Peso Devoluciones';
        comment on column stg.{self.schema}_{self.table}.quantity_order is 'Cantidad Pedido Pend.';
        comment on column stg.{self.schema}_{self.table}.quantity_delivery is 'Cantidad Entrega Pend.';
        comment on column stg.{self.schema}_{self.table}.quantity_order_delivery is 'Ped/Ent (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_check_in_month is 'A Facturar en el Mes (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_check_in_month_2 is 'A Facturar en el Resto del Mes (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_check_in_day is 'A Facturar a la Fecha (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_detained_credit is 'Retenido por Crédito (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_block_delivery is 'Bloqueo de Entrega (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_customer_collect is 'Cliente Recog (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_with_assigned_stock is 'Con Stock Asignado (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_with_stock is 'Con Stock (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_without_stock is 'Sin Stock (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_future_months is 'Meses Futuros (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_projection is 'Proy (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_projection_2 is 'Proyección en Cant.';
        comment on column stg.{self.schema}_{self.table}.quantity_plan is 'Plan (Ctd)';
        comment on column stg.{self.schema}_{self.table}.quantity_return is 'Cantidad Devoluciones';
        comment on column stg.{self.schema}_{self.table}.amount_order is 'Importe Pedido Pend.';
        comment on column stg.{self.schema}_{self.table}.amount_delivery is 'Importe Entrega Pend.';
        comment on column stg.{self.schema}_{self.table}.amount_order_delivery is 'Ped/Ent (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_check_in_month is 'A Facturar en el Mes (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_check_in_month_2 is 'A Facturar en el Resto del Mes (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_check_in_day is 'A Facturar a la Fecha (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_detained_credit is 'Retenido por Crédito (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_block_delivery is 'Bloqueo de Entrega (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_customer_collect is 'Cliente Recog (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_with_assigned_stock is 'Con Stock Asignado (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_with_stock is 'Con Stock (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_without_stock is 'Sin Stock (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_future_months is 'Meses Futuros (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_projection is 'Proy (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_projection_2 is 'Proyección en Moneda';
        comment on column stg.{self.schema}_{self.table}.amount_projection_linear is 'Proy Lineal (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_plan is 'Plan (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_return is 'Importe Devoluciones';
        comment on column stg.{self.schema}_{self.table}.quantity is 'Real (Ctd)';
        comment on column stg.{self.schema}_{self.table}.weight is 'Real (Ton)';
        comment on column stg.{self.schema}_{self.table}.amount_gross is 'Real Bruto (S/)';
        comment on column stg.{self.schema}_{self.table}.amount_subtotal is 'Real (S/)';
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

          "Número de Cliente Cod"::bigint as customer_id,
          upper(trim("Número de Cliente (Vista Comercial) Cod")) as customer_commercial_id,

          upper(trim("Número de Pedido")) as order_id,
          (case
            when "Fecha de Pedido" ~ '^\d{2}.\d{2}.\d{4}$' then to_date("Fecha de Pedido",'DD.MM.YYYY')
            else null
          end) as order_date,
          upper(trim("Clase de Pedido Cod")) as order_class_id,
          upper(trim("Motivo de Pedido Cod")) as order_reason_id,

          "Canal de Distribución Cod"::smallint as "distribution_channel_id",
          upper(trim("Tipo de Posición Cod")) as position_type_id,
          upper(trim("Material Cod")) as product_id,
          "Grupo de Vendedores Cod"::smallint as group_id,
          "Oficina de Ventas Cod"::smallint as office_id,
          "Región de Ventas Cod"::smallint as region_id,
          "Región Geográfica Cod"::smallint as region_geography_id,
          (case "Zona de Ventas Cod"
            when '#' then null
            else "Zona de Ventas Cod"
          end)::smallint as zone_id,
          "Territorio Cod"::smallint as territory_id,

          upper(trim("Clase de Factura Cod")) as invoice_type_id,
          upper(trim("Clasificación Cliente Cod")) as classification_id,
          "Organización de Ventas Cod"::smallint as organization_id,
          "Destinatario de la Mercancía Cod"::bigint as recipient_id,
          "Tipo de Valor Cod"::smallint as value_id,
          upper(trim("Tipo de Venta Cod")) as type_id,
          upper(trim("Número de Documento")) as document_id,
          upper(trim("Posición de Documento Cod")) as document_position_id,
          upper(trim("Estado de Pendiente Cod")) as status_id,
          upper(trim("Centro Cod")) as center_id,
          upper(trim("Clave de Condiciones de Pago Cod")) as condition_id,
          upper(trim("Clase de Operación Cod")) as operation_id,
          upper(trim("Unidad de Medida Base")) as unit_id,
          upper(trim("Moneda")) as coin,
          "Volumen Ventas"::numeric(18,6) as volume,
          upper(trim("UM Volumen de Ventas")) as volume_measure,
          "SD Peso Neto"::numeric(18,6) as net_weight,
          "Ingreso Neto"::numeric(18,6) as net_income,
          "Ingreso Bruto"::numeric(18,6) as gross_income,

          upper(trim("InfoProvider")) as provider_id,
          upper(trim("Flag_Estado")) as flag_status,
          upper(trim("Flag_Estado2")) as flag_status_2,
          "Días H Restantes"::numeric(18,6) as remain_days,
          "Días Hábiles"::numeric(18,6) as business_days,
          upper(trim("Oficina de Ventas Nivel 1")) as sales_office_1,
          upper(trim("Oficina de Ventas Nivel 2")) as sales_office_2,
          upper(trim("Periodo en Curso")) as period_current,
          upper(trim("Periodo Cerrado")) as period_closed,

          "Peso Pedido Pend."::numeric(18,6) as weight_order,
          "Peso Entrega Pend."::numeric(18,6) as weight_delivery,
          "Ped/Ent (Ton)"::numeric(18,6) as weight_order_delivery,
          "A Facturar en el Mes (Ton)"::numeric(18,6) as weight_check_in_month,
          "A Facturar en el Resto del Mes (Ton)"::numeric(18,6) as weight_check_in_month_2,
          "A Facturar a la Fecha (Ton)"::numeric(18,6) as weight_check_in_day,
          "Retenido por Crédito (Ton)"::numeric(18,6) as weight_detained_credit,
          "Bloqueo de Entrega (Ton)"::numeric(18,6) as weight_block_delivery,
          "Cliente Recog (Ton)"::numeric(18,6) as weight_customer_collect,
          "Con Stock Asignado (Ton)"::numeric(18,6) as weight_with_assigned_stock,
          "Con Stock (Ton)"::numeric(18,6) as weight_with_stock,
          "Sin Stock (Ton)"::numeric(18,6) as weight_without_stock,
          "Meses Futuros (Ton)"::numeric(18,6) as weight_future_months,
          "Proy (Ton)"::numeric(18,6) as weight_projection,
          "Proyección en Peso"::numeric(18,6) as weight_projection_2,
          "Proy Lineal (Ton)"::numeric(18,6) as weight_projection_linear,
          "Plan (Ton)"::numeric(18,6) as weight_plan,
          "Peso Devoluciones"::numeric(18,6) as weight_return,

          "Cantidad Pedido Pend."::numeric(18,6) as quantity_order,
          "Cantidad Entrega Pend."::numeric(18,6) as quantity_delivery,
          "Ped/Ent (Ctd)"::numeric(18,6) as quantity_order_delivery,
          "A Facturar en el Mes (Ctd)"::numeric(18,6) as quantity_check_in_month,
          "A Facturar en el Resto del Mes (Ctd)"::numeric(18,6) as quantity_check_in_month_2,
          "A Facturar a la Fecha (Ctd)"::numeric(18,6) as quantity_check_in_day,
          "Retenido por Crédito (Ctd)"::numeric(18,6) as quantity_detained_credit,
          "Bloqueo de Entrega (Ctd)"::numeric(18,6) as quantity_block_delivery,
          "Cliente Recog (Ctd)"::numeric(18,6) as quantity_customer_collect,
          "Con Stock Asignado (Ctd)"::numeric(18,6) as quantity_with_assigned_stock,
          "Con Stock (Ctd)"::numeric(18,6) as quantity_with_stock,
          "Sin Stock (Ctd)"::numeric(18,6) as quantity_without_stock,
          "Meses Futuros (Ctd)"::numeric(18,6) as quantity_future_months,
          "Proy (Ctd)"::numeric(18,6) as quantity_projection,
          "Proyección en Cant."::numeric(18,6) as quantity_projection_2,
          "Plan (Ctd)"::numeric(18,6) as quantity_plan,
          "Cantidad Devoluciones"::numeric(18,6) as quantity_return,

          "Importe Pedido Pend."::numeric(18,6) as amount_order,
          "Importe Entrega Pend."::numeric(18,6) as amount_delivery,
          "Ped/Ent (S/)"::numeric(18,6) as amount_order_delivery,
          "A Facturar en el Mes (S/)"::numeric(18,6) as amount_check_in_month,
          "A Facturar en el Resto del Mes (S/)"::numeric(18,6) as amount_check_in_month_2,
          "A Facturar a la Fecha (S/)"::numeric(18,6) as amount_check_in_day,
          "Retenido por Crédito (S/)"::numeric(18,6) as amount_detained_credit,
          "Bloqueo de Entrega (S/)"::numeric(18,6) as amount_block_delivery,
          "Cliente Recog (S/)"::numeric(18,6) as amount_customer_collect,
          "Con Stock Asignado (S/)"::numeric(18,6) as amount_with_assigned_stock,
          "Con Stock (S/)"::numeric(18,6) as amount_with_stock,
          "Sin Stock (S/)"::numeric(18,6) as amount_without_stock,
          "Meses Futuros (S/)"::numeric(18,6) as amount_future_months,
          "Proy (S/)"::numeric(18,6) as amount_projection,
          "Proyección en Moneda"::numeric(18,6) as amount_projection_2,
          "Proy Lineal (S/)"::numeric(18,6) as amount_projection_linear,
          "Plan (S/)"::numeric(18,6) as amount_plan,
          "Importe Devoluciones"::numeric(18,6) as amount_return,

          "Real (Ctd)"::numeric(18,6) as quantity,
          "Real (Ton)"::numeric(18,6) as weight,
          "Real Bruto (S/)"::numeric(18,6) as amount_gross,
          "Real (S/)"::numeric(18,6) as amount_subtotal
        from
          raw.{self.schema}_{self.table}
        where
          "Número de Cliente Cod" ~ '^\d+$' and
          "Día Natural" is not null
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

        comment on table {self.schema}.{self.table} is 'Sales Direct Table: [lib://QVDs Alicorp (grupoalicorp_mninapi)/Avance de Ventas Alicorp\ZQV_ZSD_M001_Q01_YYYYMMDD.QVD]';

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
          file_name = '{date}.csv'
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
        from stg.{self.schema}_{self.table}
      '''
      self._cursor.execute(sql)
      self._client.commit()
    except Exception as e:
      print('error:',repr(e))
      raise e