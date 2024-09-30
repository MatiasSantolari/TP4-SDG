import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, select
from dotenv import load_dotenv


class ETLProcess:
    def __init__(self):
        self.dimension_clientes = pd.DataFrame()
        self.dimension_empleados = pd.DataFrame()
        self.regiones = pd.DataFrame()
        self.dimension_productos = pd.DataFrame()
        self.dimension_tiempo = pd.DataFrame()
        self.ventas = pd.DataFrame()

        load_dotenv()
        database_url = (
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        self.engine = create_engine(database_url)
        self.metadata = MetaData()

    def convertir_txt_a_csv(self):
        try:
            with open('data/Regions.txt', 'r') as file:
                lines = file.readlines()
            lines.insert(0, "REGION|STATE|CITY|ZIPCODE\n")
            temp_file_path = 'data/temp_Regions.csv'
            with open(temp_file_path, 'w') as file:
                file.writelines(lines)
            self.regiones = pd.read_csv(temp_file_path, delimiter='|')
            self.regiones.to_csv('data/Regions.csv', index=False, sep=',')
            os.remove(temp_file_path)
            logging.info('Conversión de Regions.txt a Regions.csv completada.')
        except Exception as e:
            logging.error(f'Error al convertir Regions.txt a CSV: {e}')

    def cargar_dimension_tiempo_ventas(self):
        try:
            engine = create_engine(
                f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
            billing = Table('billing', self.metadata, autoload_with=engine)
            billing_detail = Table('billing_detail', self.metadata, autoload_with=engine)
            prices = Table('prices', self.metadata, autoload_with=engine)

            query = select(
                billing.c.BILLING_ID.label('billing_id'),
                billing.c.REGION.label('region'),
                billing.c.BRANCH_ID.label('branch_id'),
                billing.c.DATE.label('date'),
                billing.c.CUSTOMER_ID.label('customer_id'),
                billing.c.EMPLOYEE_ID.label('employee_id'),
                billing_detail.c.PRODUCT_ID.label('product_id'),
                billing_detail.c.QUANTITY.label('quantity'),
                prices.c.DATE.label('date_price'),
                prices.c.PRICE.label('price')
            ).select_from(
                billing.join(billing_detail, billing.c.BILLING_ID == billing_detail.c.BILLING_ID)
                .join(prices, billing_detail.c.PRODUCT_ID == prices.c.PRODUCT_ID)
            )

            with engine.connect() as connection:
                result_proxy = connection.execute(query)
                billing_data = result_proxy.fetchall()

            billing_df = pd.DataFrame(billing_data, columns=[
                'billing_id', 'region', 'branch_id', 'date', 'customer_id',
                'employee_id', 'product_id', 'quantity', 'date_price', 'price'
            ])
            billing_df['date'] = pd.to_datetime(billing_df['date'], errors='coerce')
            billing_df.dropna(subset=['date'], inplace=True)

            self.dimension_tiempo = pd.DataFrame({
                'id_tiempo': range(1, len(billing_df) + 1),
                'fecha': billing_df['date'],
                'día': billing_df['date'].dt.day,
                'mes': billing_df['date'].dt.month,
                'trimestre': billing_df['date'].dt.quarter,
                'año': billing_df['date'].dt.year
            })
            self.dimension_tiempo.drop_duplicates(subset=['fecha'], inplace=True)

            self.ventas = pd.DataFrame({
                'id_tiempo': self.dimension_tiempo['id_tiempo'],
                'id_empleado': billing_df['customer_id'],
                'id_vendedor': billing_df['employee_id'],
                'id_producto': billing_df['product_id'],
                'cantidad': billing_df['quantity'],
                'precio': billing_df['price']
            })

            logging.info('Tabla de ventas creada con éxito.')
        except Exception as e:
            logging.error(f'Error al cargar la dimensión de tiempo y las ventas: {e}')

    def cargar_clientes(self):
        try:
            customer_r = pd.read_xml('data/Customer_R.xml')
            customer_w = pd.read_xml('data/Customer_W.xml')

            self.convertir_txt_a_csv()
            self.cargar_regiones()
            customer_r = self.traducir_cliente(customer_r, 'minorista')
            customer_w = self.traducir_cliente(customer_w, 'mayorista')

            self.dimension_clientes = pd.concat([customer_r, customer_w], ignore_index=True)
            self.validar_datos(self.dimension_clientes)
            logging.info('Clientes cargados y validados.')
        except Exception as e:
            logging.error(f'Error al procesar los archivos de clientes: {e}')

    def cargar_regiones(self):
        try:
            self.regiones = pd.read_csv('data/Regions.csv', delimiter=',')
            logging.info('Regiones cargadas.')
        except Exception as e:
            logging.error(f'Error al procesar Regions.csv: {e}')

    def cargar_empleados(self):
        try:
            self.dimension_empleados = pd.read_excel('data/Employee.xls')
            self.dimension_empleados = self.traducir_empleado(self.dimension_empleados)
            self.validar_datos(self.dimension_empleados, check_column='antiguedad')
            logging.info('Empleados cargados y validados.')
        except Exception as e:
            logging.error(f'Error al procesar Employee.xls: {e}')

    def cargar_productos(self):
        try:
            products = pd.read_csv('data/Products.txt', delimiter='|', names=['product_id', 'detalle', 'tipoEnvase'])
            df_products = products.copy()
            df_products['product_id'] = df_products['product_id'].astype(str).str.rstrip().astype(int)
            df_products['detalle'] = df_products['detalle'].str.rstrip()
            df_products['old_package'] = df_products['tipoEnvase']
            df_products['tipoEnvase'] = np.where(df_products['tipoEnvase'].str.contains('can'), 'Can', 'Bottle')

            def transform_liters(lst):
                if lst[1] == 'Liter':
                    return float(lst[0])
                elif lst[1] == 'cm3':
                    return float(lst[0]) / 1000
                return np.nan

            df_products['splitted'] = df_products['old_package'].str.split(' ')
            df_products['litros'] = df_products['splitted'].apply(transform_liters)
            df_products.drop(columns=['old_package', 'splitted'], inplace=True)

            def clasificar_bebida(detalle):
                detalle = detalle.lower()
                if 'diet' in detalle:
                    return 'Bebida de dieta'
                if 'caffeine' in detalle:
                    return 'Bebida de cafeína'
                if 'energy' in detalle:
                    return 'Bebida energética'
                if 'kool' in detalle:
                    return 'Bebida Kool'
                if 'root' in detalle:
                    return 'Bebida Root'
                if 'juice' in detalle:
                    return 'Jugo'
                if 'soda' in detalle:
                    return 'Bebida de soda'
                return 'Otro tipo de bebida'

            df_products['tipoBebida'] = df_products['detalle'].apply(clasificar_bebida)
            self.dimension_productos = df_products
            logging.info('Productos cargados y procesados.')
        except Exception as e:
            logging.error(f'Error al cargar productos: {e}')

    def guardar_datos(self):
        self.guardar_csv(self.dimension_clientes, 'dimension_clientes.csv')
        self.guardar_csv(self.dimension_empleados, 'dimension_vendedores.csv')
        self.guardar_csv(self.dimension_productos, 'dimension_productos.csv')
        self.guardar_csv(self.dimension_tiempo, 'dimension_tiempo.csv')
        self.guardar_csv(self.ventas, 'tabla_hecho_ventas.csv')

    @staticmethod
    def guardar_csv(df, filename):
        if os.path.exists(filename):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base, ext = os.path.splitext(filename)
            new_filename = f"{base}_{timestamp}{ext}"
            logging.info(f"El archivo {filename} ya existe. Se guardará como {new_filename}.")
            df.to_csv(new_filename, index=False)
        else:
            df.to_csv(filename, index=False)

    @staticmethod
    def validar_datos(df, check_column=None):
        if df.isnull().sum().any():
            logging.warning("Hay valores nulos en las siguientes columnas:")
            logging.warning(df.isnull().sum())
            if check_column and df[check_column].isnull().sum() > 0:
                logging.warning(f"Valores nulos en la columna '{check_column}':")
                logging.warning(df[df[check_column].isnull()])
        else:
            logging.info("No hay valores nulos en los datos.")

    def traducir_cliente(self, df, tipo_cliente):
        df['tipo_cliente'] = tipo_cliente
        df['BIRTH_DATE'] = pd.to_datetime(df['BIRTH_DATE'], errors='coerce')
        df['CITY'] = df['CITY'].str.lower().str.strip()
        df['STATE'] = df['STATE'].str.lower().str.strip()
        self.regiones['CITY'] = self.regiones['CITY'].str.lower().str.strip()
        self.regiones['STATE'] = self.regiones['STATE'].str.lower().str.strip()

        df['zona'] = None
        df['rango_edad'] = None

        for index, row in df.iterrows():
            ciudad = row['CITY']
            estado = row['STATE']
            region = self.regiones.loc[(self.regiones['CITY'] == ciudad) & (self.regiones['STATE'] == estado)]
            if not region.empty:
                df.at[index, 'zona'] = region['REGION'].values[0].strip()
            else:
                logging.warning(f"No se encontró región para la ciudad: {ciudad} y estado: {estado}")

            birth_date = row['BIRTH_DATE']
            if pd.isnull(birth_date):
                df.at[index, 'rango_edad'] = 'Desconocido'
            else:
                age = self.calcular_edad(birth_date)
                if age < 20:
                    df.at[index, 'rango_edad'] = 'Menos de 20'
                elif 20 <= age < 40:
                    df.at[index, 'rango_edad'] = '20 a 40'
                elif 40 <= age < 60:
                    df.at[index, 'rango_edad'] = '40 a 60'
                else:
                    df.at[index, 'rango_edad'] = 'Más de 60'

        no_macheos = df[df['zona'].isnull()]
        if not no_macheos.empty:
            logging.info("Registros sin coincidencia en la zona:")
            logging.info(no_macheos[['CUSTOMER_ID', 'FULL_NAME', 'CITY', 'STATE']])
        else:
            logging.info("Todos los clientes tienen coincidencia en la zona.")

        return df.rename(columns={
            'CUSTOMER_ID': 'id_cliente',
            'FULL_NAME': 'nombre',
            'CITY': 'ciudad',
            'STATE': 'provincia',
            'zona': 'zona',
            'rango_edad': 'rango_edad'
        })[['id_cliente', 'nombre', 'ciudad', 'provincia', 'zona', 'tipo_cliente', 'rango_edad']]

    @staticmethod
    def calcular_edad(fecha_nacimiento):
        if pd.isnull(fecha_nacimiento):
            return None
        today = datetime.today()
        return today.year - fecha_nacimiento.year - ((today.month, today.day) < (fecha_nacimiento.month, fecha_nacimiento.day))

    @staticmethod
    def traducir_empleado(df):
        df = df.rename(columns={
            'EMPLOYEE_ID': 'id_vendedor',
            'FULL_NAME': 'nombre',
            'GENDER': 'sexo',
            'EMPLOYMENT_DATE': 'fecha_empleo',
            'BIRTH_DATE': 'fecha_nacimiento'
        })

        df['fecha_empleo'] = pd.to_datetime(df['fecha_empleo'], errors='coerce')
        df['antiguedad'] = (datetime.today() - df['fecha_empleo']).dt.days // 365
        df['sexo'] = df['sexo'].replace({'M': 'Masculino', 'F': 'Femenino'})
        df['zona'] = np.where(df['antiguedad'] < 1, 'Nuevo',
                              np.where(df['antiguedad'] < 5, 'Medio', 'Antiguo'))

        return df[['id_vendedor', 'nombre', 'sexo', 'antiguedad', 'fecha_nacimiento', 'zona']]


etl = ETLProcess()
etl.cargar_clientes()
etl.cargar_empleados()
etl.cargar_productos()
etl.cargar_dimension_tiempo_ventas()
etl.guardar_datos()
