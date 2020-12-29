from google.cloud import storage as gcs
import os
import pip
import platform
import re

from src import (
  Database,
  GoogleCloudStorage,
  HdSalesDex,
  HdSalesDirect,
  LogFile,
  LogSource,
  MuCustomersCommercial,
  MuCustomersDex,
  MuProducts,
  MuTerritoriesDex,
  MwCustomersDex,

  add_days,
  cast_date_to_YYYYMMDD,
  cast_YYYYMMDD_to_date,
  change_utc_to_tz,
  get_days_between_two_dates,
  get_env,
  load_yml_gcf,
  load_yml_local,
  get_lines_from_binary_file,
  get_now_utc
)

def main(data,context=None):
  # check version
  print('')
  print(f'python: {platform.python_version()}')#3.7.7
  print(f'pip: {pip.__version__}')#20.0.2

  try:
    print('')
    print(f'data: {data}')
    print(f'context: {context}')

    bucket = data['bucket']
    filename = data['name']
    print('')
    print(f'bucket: {bucket}')
    print(f'filename: {filename}')

    # storage
    options = {
      'customers-commercial': re.compile('customers-commercial/raw/.{4}/.{8}.csv'),
      'customers-dex': re.compile('customers-dex/raw/.{4}/.{8}.csv'),
      'products': re.compile('products/raw/.{4}/.{8}.csv'),
      'sales-dex': re.compile('sales-dex/raw/.{4}/.{8}.csv'),
      'sales-direct': re.compile('sales-direct/raw/.{4}/.{8}.csv'),
      'territories-dex': re.compile('territories-dex/raw/.{4}/.{8}.csv')
    }
    flag = False
    for option in options.values():
      if option.match(filename):
        client_gcs = gcs.Client()
        flag = True
        break

    if flag:
      blob = GoogleCloudStorage(client_gcs,bucket,filename)
    else:
      return

    # database
    database_data = Database()
    database_log = Database()
    if env == 'gcf':
      database_data.connect(
        host=os.environ['DB_DATA_HOST'],
        port=os.environ['DB_DATA_PORT'],
        user=os.environ['DB_DATA_USER'],
        password=os.environ['DB_DATA_PASS'],
        database=os.environ['DB_DATA_DATABASE'],
        application_name=os.environ['DB_DATA_APPLICATION_NAME']
      )
      database_log.connect(
        host=os.environ['DB_LOG_HOST'],
        port=os.environ['DB_LOG_PORT'],
        user=os.environ['DB_LOG_USER'],
        password=os.environ['DB_LOG_PASS'],
        database=os.environ['DB_LOG_DATABASE'],
        application_name=os.environ['DB_DATA_APPLICATION_NAME']
      )
    elif env == 'local':
      database_data.connect(
        host=os.environ['DB_DATA_HOST'],
        port=os.environ['DB_DATA_PORT'],
        user=os.environ['DB_DATA_USER'],
        password=os.environ['DB_DATA_PASS'],
        database=os.environ['DB_DATA_DATABASE'],
        application_name=os.environ['DB_DATA_APPLICATION_NAME'],
        ssl_mode=os.environ['DB_DATA_SSL_MODE'],
        ssl_server_ca=os.environ['DB_DATA_SSL_SERVER_CA'],
        ssl_client_cert=os.environ['DB_DATA_SSL_CLIENT_CERT'],
        ssl_client_key=os.environ['DB_DATA_SSL_CLIENT_KEY']
      )
      database_log.connect(
        host=os.environ['DB_LOG_HOST'],
        port=os.environ['DB_LOG_PORT'],
        user=os.environ['DB_LOG_USER'],
        password=os.environ['DB_LOG_PASS'],
        database=os.environ['DB_LOG_DATABASE'],
        application_name=os.environ['DB_DATA_APPLICATION_NAME']
      )

    database_data.load_config()
    database_data.set_cursor()
    db_data = {
      'client': database_data.get_client(),
      'cursor': database_data.get_cursor()
    }
    database_log.load_config()
    database_log.set_cursor()
    db_log = {
      'client': database_log.get_client(),
      'cursor': database_log.get_cursor()
    }

    # ETL
    log_file = LogFile(db_log['client'],db_log['cursor'])
    log = log_file.find_by_bucket_filename({'bucket':bucket,'filename':filename})
    folder = filename.split('/')[0]
    date = filename.split('/')[-1].split('.')[0]

    print('')
    if log:
      status = log['status']
      print(f'{folder} record exists in log_file table')
      print(f'  status: {status}')
    else:
      status = False
      print(f'{folder} record doesn\'t exist in log_file table')
      log_source = LogSource(db_log['client'],db_log['cursor'])
      source = log_source.find_by_bucket_name({'bucket':bucket,'name':folder})
      if source:
        log_file.create({
          'source_id': source['id'],
          'bucket': bucket,
          'filename': filename,
          'date': date,
          'trigger': 'cloud-function',
          'rows': 0,
          'status': False
        })
      else:
        print(f'{folder} record doesn\'t exist in log_sources table')
        return

    print('')
    print('downloading...')
    file_bytes = blob.download_to_bytesio()
    rows = get_lines_from_binary_file(file_bytes)
    print(f'rows: {rows:,}')

    log_file.update({'bucket':bucket,'filename':filename,'rows':rows,'status':False,'created_at':get_now_utc()})

    print('')
    if folder == 'customers-commercial':
      mu_customers_commercial = MuCustomersCommercial(db_data['client'],db_data['cursor'])
      print('loading raw.qlik_mu_customers_commercial')
      mu_customers_commercial.create_raw()
      mu_customers_commercial.load_raw(file_bytes)
      print('loading stg.qlik_mu_customers_commercial')
      mu_customers_commercial.create_stg()
      mu_customers_commercial.transformation_stg()
      print('loading qlik.mu_mu_customers_commercial')
      mu_customers_commercial.create()
      mu_customers_commercial.truncate()
      mu_customers_commercial.load()
    elif folder == 'customers-dex':
      mu_customer_dex = MuCustomersDex(db_data['client'],db_data['cursor'])
      print('loading raw.qlik_mu_customers_dex')
      mu_customer_dex.create_raw()
      mu_customer_dex.load_raw(file_bytes)
      print('loading stg.qlik_mu_customers_dex')
      mu_customer_dex.create_stg()
      mu_customer_dex.transformation_stg()
      print('loading qlik.mu_customers_dex')
      mu_customer_dex.create()
      mu_customer_dex.truncate()
      mu_customer_dex.load()

      mw_customer_dex = MwCustomersDex(db_data['client'],db_data['cursor'])
      print('loading qlik.mw_customers_dex')
      mw_customer_dex.create()
      mw_customer_dex.clean(date)
      mw_customer_dex.load(date)
    elif folder == 'products':
      mu_products = MuProducts(db_data['client'],db_data['cursor'])
      print('loading raw.qlik_mu_products')
      mu_products.create_raw()
      mu_products.load_raw(file_bytes)
      print('loading stg.qlik_mu_products')
      mu_products.create_stg()
      mu_products.transformation_stg()
      print('loading qlik.mu_products')
      mu_products.create()
      mu_products.truncate()
      mu_products.load()
    elif folder == 'sales-dex':
      hd_sales_dex = HdSalesDex(db_data['client'],db_data['cursor'])
      print('loading raw.qlik_hd_sales_dex')
      hd_sales_dex.create_raw()
      if date < '20200501':
        columns = '"Cliente DEX Cod","Negocio","Gerencia Regional Cod","Región Cod","Zona de Ventas Cod","Número de Cliente (vista comercial) Cod","Canal de Distribución Cod","Negocio Cod","Tipo de Posición de Documento Comercial Cod","Material Cod","Organización de Ventas Cod","Grupo de Vendedores Cod","Oficina de Ventas Cod","Tipo Valor para Informes Cod","Segmento Cod","Fuente Cod","Personal Vendedor DEX Cod","Tipo de Venta Telefónica DEX Cod","Territorio DEX Cod","Giro del Cliente Cod","Territorio Cod","Día Natural","Ejercicio/Período Cod","Gerencia Regional","Región","Zona de Ventas","Número de Cliente (vista comercial)","Canal de Distribución","Material","Organización de Ventas","Grupo de Vendedores","Oficina de Ventas","Tipo Valor para Informes","Cliente DEX","Segmento","Fuente","Personal Vendedor DEX","Tipo de Venta Telefónica DEX","Giro del Cliente","Territorio","Año/Semana Natural","Ejercicio/Período","Ventas Reales (S/)","Ventas Real Bruto (S/)","Ventas Plan (S/)","Peso Real (Ton)","Peso Plan (Ton)","Real (Ctd)","Plan (Ctd)","Negocio Nivel 2","Negocio Nivel 1","Impuesto","Proyección (Ctd)","Proyección (S/)","Proyección (Ton)","Proy Lineal (S/)","Proy Lineal (Ton)","Proy Lineal (Ctd)","Ventas Reales con IGV (S/)","Ventas Plan con IGV (S/)","Proy Lineal con IGV (S/)","Día","Año","Des_Mes","Cod_Mes","Periodo","Negocio_IC","Plataforma"'
      else:
        columns = '"Cliente DEX Cod","Negocio","Gerencia Regional Cod","Región Cod","Zona de Ventas Cod","Número de Cliente (vista comercial) Cod","Canal de Distribución Cod","Negocio Cod","Tipo de Posición de Documento Comercial Cod","Material Cod","Organización de Ventas Cod","Grupo de Vendedores Cod","Oficina de Ventas Cod","Tipo Valor para Informes Cod","Segmento Cod","Fuente Cod","Personal Vendedor DEX Cod","Tipo de Venta Telefónica DEX Cod","Territorio DEX Cod","Giro del Cliente Cod","Territorio Cod","Día Natural","Ejercicio/Período Cod","Gerencia Regional","Región","Zona de Ventas","Número de Cliente (vista comercial)","Canal de Distribución","Material","Organización de Ventas","Grupo de Vendedores","Oficina de Ventas","Tipo Valor para Informes","Cliente DEX","Segmento","Fuente","Personal Vendedor DEX","Tipo de Venta Telefónica DEX","Giro del Cliente","Territorio","Año/Semana Natural","Ejercicio/Período","Ventas Reales (S/)","Ventas Real Bruto (S/)","Ventas Plan (S/)","Peso Real (Ton)","Peso Plan (Ton)","Real (Ctd)","Plan (Ctd)","Negocio Nivel 2","Negocio Nivel 1","Impuesto","Proyección (Ctd)","Proyección (S/)","Proyección (Ton)","Proy Lineal (S/)","Proy Lineal (Ton)","Proy Lineal (Ctd)","Ventas Reales con IGV (S/)","Ventas Plan con IGV (S/)","Proy Lineal con IGV (S/)","Día","Año","Des_Mes","Cod_Mes","Periodo","Negocio_IC","Plataforma","Flag Pedido Web Cod","Flag Pedido Web"'
      hd_sales_dex.load_raw(file_bytes,columns)
      print('loading stg.qlik_hd_sales_dex')
      hd_sales_dex.create_stg()
      hd_sales_dex.transformation_stg(date)
      print('loading qlik.hd_sales_dex')
      hd_sales_dex.create()
      hd_sales_dex.clean(date)
      hd_sales_dex.load()

      '''
      product_sales = ProductSales(db_data['client'],db_data['cursor'])
      print('updating dex.products_sales')
      product_sales.update_dm(date)
      print('inserting dex.products_sales')
      product_sales.insert_dm()
      print('cleaning dex.products_sales')
      product_sales.clean()

      customer_sales = CustomerSales(db_data['client'],db_data['cursor'])
      print('updating dex.customers_sales')
      customer_sales.update_dm(date)
      print('inserting dex.customers_sales')
      customer_sales.insert_dm()
      print('cleaning dex.customers_sales')
      customer_sales.clean()
      '''
    elif folder == 'sales-direct':
      hd_sales_direct = HdSalesDirect(db_data['client'],db_data['cursor'])
      print('loading raw.qlik_hd_sales_direct')
      hd_sales_direct.create_raw()
      hd_sales_direct.load_raw(file_bytes)
      print('loading stg.qlik_hd_sales_direct')
      hd_sales_direct.create_stg()
      hd_sales_direct.transformation_stg(date)
      print('loading qlik.hd_sales_direct')
      hd_sales_direct.create()
      hd_sales_direct.clean(date)
      hd_sales_direct.load()
    elif folder == 'territories-dex':
      mu_territories_dex = MuTerritoriesDex(db_data['client'],db_data['cursor'])
      print('loading raw.qlik_mu_territories_dex')
      mu_territories_dex.create_raw()
      mu_territories_dex.load_raw(file_bytes)
      print('loading stg.qlik_mu_territories_dex')
      mu_territories_dex.create_stg()
      mu_territories_dex.transformation_stg()
      print('loading qlik.mu_territories_dex')
      mu_territories_dex.create()
      mu_territories_dex.truncate()
      mu_territories_dex.load()

    # log
    log_file.update({'bucket':bucket,'filename':filename,'status':True})

    print('')
    print('done')
  except Exception as e:
    print('')
    print(repr(e))
  finally:
    if 'database_data' in dir(): database_data.disconnect()
    if 'database_log' in dir(): database_log.disconnect()

env = get_env()
if env == 'gcf':
  load_yml_gcf()
elif env == 'local':
  #os.system('pip list --outdated > requirements-outdated.txt')
  load_yml_local()
  folder = 'sales-dex'

  if folder == 'customers-commercial':
    main({
      'bucket': 'alicorp-pe-is-qlik-gcs',
      'name': 'customers-commercial/raw/2020/20200817.csv'
    })
  elif folder == 'customers-dex':
    main({
      'bucket': 'alicorp-pe-is-qlik-gcs',
      'name': 'customers-dex/raw/2020/20200817.csv'
    })
  elif folder == 'products':
    main({
      'bucket': 'alicorp-pe-is-qlik-gcs',
      'name': 'products/raw/2020/20200817.csv'
    })
  elif folder == 'sales-dex':
    if 1 == 1:
      main({
        'bucket': 'alicorp-pe-is-qlik-gcs',
        'name': 'sales-dex/raw/2020/20200816.csv'
      })
    else:
      begin_date = cast_YYYYMMDD_to_date('20200505')
      end_date = cast_YYYYMMDD_to_date('20200510')
      for d in get_days_between_two_dates(begin_date,end_date):
        date = cast_date_to_YYYYMMDD(d)
        main({
          'bucket': 'alicorp-pe-is-qlik-gcs',
          'name':f'sales-dex/raw/{date[:4]}/{date}.csv'
        })
  elif folder == 'sales-direct':
    main({
      'bucket': 'alicorp-pe-is-qlik-gcs',
      'name': 'sales-direct/raw/2020/20200817.csv'
    })
  elif folder == 'territories-dex':
    main({
      'bucket': 'alicorp-pe-is-qlik-gcs',
      'name': 'territories-dex/raw/2020/20200817.csv'
    })