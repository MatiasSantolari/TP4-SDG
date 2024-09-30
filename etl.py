import logging
import os
import subprocess
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    ForeignKey,
    MetaData,
    Table,
    create_engine,
    select,
    text,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.mysql import DATE, FLOAT, INTEGER, VARCHAR
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import CreateTable

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ETLProcess:
    """
    Clase que realiza el proceso ETL (Extract, Transform, Load) para datos de clientes, empleados,
    productos, ventas y regiones, y maneja la inserción en una base de datos MySQL.
    """

    def __init__(self):
        """
        Inicializa la clase ETLProcess cargando las variables de entorno, configurando las conexiones
        a la base de datos de origen y destino, y preparando los DataFrames vacíos.
        """
        # DataFrames
        self.dimension_clientes = pd.DataFrame()
        self.dimension_empleados = pd.DataFrame()
        self.regiones = pd.DataFrame()
        self.dimension_productos = pd.DataFrame()
        self.dimension_tiempo = pd.DataFrame()
        self.ventas = pd.DataFrame()

        load_dotenv()

        # Conexión al servidor MySQL sin especificar la base de datos
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')

        # Nombre de la base de datos origen y destino
        self.db_name_origin = os.getenv('DB_NAME')
        self.db_name_dest = 'tp4_dh'

        # Crear la cadena de conexión sin base de datos
        self.database_url = (
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}"
        )
        self.engine = create_engine(self.database_url)
        self.metadata = MetaData()

        # Crear la base de datos destino si no existe
        self.crear_base_de_datos()

        # Conectar al engine de origen y destino
        self.engine_origin = create_engine(f"{self.database_url}/{self.db_name_origin}")
        self.engine_dest = create_engine(f"{self.database_url}/{self.db_name_dest}")
        self.metadata.bind = self.engine_dest

    def crear_base_de_datos(self):
        """
        Crea la base de datos destino si no existe. Si ya existe, la elimina y la vuelve a crear.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {self.db_name_dest}"))
                logging.info(f"La base de datos '{self.db_name_dest}' ha sido eliminada si existía.")
                conn.execute(text(f"CREATE DATABASE {self.db_name_dest}"))
                logging.info(f"La base de datos '{self.db_name_dest}' ha sido creada.")
        except Exception as e:
            logging.error(f"Error al crear la base de datos '{self.db_name_dest}': {e}")
            raise

    def generar_esquema_sql(self):
        """
        Genera el esquema SQL para las tablas del modelo dimensional (dimensiones y tabla de hechos)
        y las crea en la base de datos de destino.
        """
        try:
            self.metadata = MetaData()

            # Definición de las tablas
            dimension_clientes_table = Table(
                'dimension_clientes', self.metadata,
                Column('id_cliente', INTEGER, primary_key=True, autoincrement=False),
                Column('nombre', VARCHAR(255)),
                Column('ciudad', VARCHAR(100)),
                Column('provincia', VARCHAR(100)),
                Column('zona', VARCHAR(100)),
                Column('tipo_cliente', VARCHAR(100)),
                Column('rango_edad', VARCHAR(50))
            )

            dimension_empleados_table = Table(
                'dimension_vendedores', self.metadata,
                Column('id_vendedor', INTEGER, primary_key=True, autoincrement=False),
                Column('nombre', VARCHAR(255)),
                Column('sexo', VARCHAR(50)),
                Column('antiguedad', INTEGER),
                Column('fecha_nacimiento', DATE),
                Column('zona', VARCHAR(55))
            )

            dimension_productos_table = Table(
                'dimension_productos', self.metadata,
                Column('product_id', INTEGER, primary_key=True, autoincrement=False),
                Column('detalle', VARCHAR(255)),
                Column('tipoEnvase', VARCHAR(50)),
                Column('litros', FLOAT),
                Column('tipoBebida', VARCHAR(50))
            )

            dimension_tiempo_table = Table(
                'dimension_tiempo', self.metadata,
                Column('id_tiempo', INTEGER, primary_key=True, autoincrement=False),
                Column('fecha', DATE),
                Column('dia', INTEGER),
                Column('mes', INTEGER),
                Column('trimestre', INTEGER),
                Column('año', INTEGER)
            )

            ventas_table = Table(
                'tabla_hecho_ventas', self.metadata,
                Column('id_venta', INTEGER, primary_key=True, autoincrement=True),
                Column('id_tiempo', INTEGER, ForeignKey('dimension_tiempo.id_tiempo')),
                Column('id_cliente', INTEGER, ForeignKey('dimension_clientes.id_cliente')),
                Column('id_vendedor', INTEGER, ForeignKey('dimension_vendedores.id_vendedor')),
                Column('id_producto', INTEGER, ForeignKey('dimension_productos.product_id')),
                Column('cantidad', INTEGER),
                Column('precio', FLOAT)
            )

            # Crear todas las tablas en la base de datos destino
            self.metadata.create_all(self.engine_dest)
            logging.info("Las tablas han sido creadas en la base de datos.")

            # Generar las sentencias CREATE TABLE para MySQL y guardarlas en un archivo
            mysql_dialect = mysql.dialect()
            tables = [
                dimension_clientes_table,
                dimension_empleados_table,
                dimension_productos_table,
                dimension_tiempo_table,
                ventas_table
            ]

            with open('esquema.sql', 'w', encoding='utf-8') as f:
                for table in tables:
                    create_table_sql = str(CreateTable(table).compile(dialect=mysql_dialect))
                    f.write(f"{create_table_sql};\n\n")
                    logging.info(f"Código SQL para la tabla '{table.name}' generado.")
            logging.info("El esquema SQL ha sido generado y guardado en 'esquema.sql'.")
        except Exception as e:
            logging.error(f"Error al generar el esquema SQL: {e}")
            raise

    def insertar_datos(self):
        """
        Inserta los datos en las tablas de la base de datos destino, asegurando la eliminación de duplicados
        y la correcta conversión de fechas.
        """
        try:
            dataframes = {
                'dimension_clientes': self.dimension_clientes,
                'dimension_vendedores': self.dimension_empleados,
                'dimension_productos': self.dimension_productos,
                'dimension_tiempo': self.dimension_tiempo,
                'tabla_hecho_ventas': self.ventas
            }

            for table_name, df in dataframes.items():
                if not df.empty:
                    # Eliminar duplicados basados en la clave primaria
                    df.drop_duplicates(subset=[df.columns[0]], inplace=True)

                    # Convertir fechas antes de la inserción
                    if 'fecha_nacimiento' in df.columns:
                        df['fecha_nacimiento'] = pd.to_datetime(
                            df['fecha_nacimiento'], errors='coerce'
                        ).dt.strftime('%Y-%m-%d')
                    if 'fecha_empleo' in df.columns:
                        df['fecha_empleo'] = pd.to_datetime(
                            df['fecha_empleo'], errors='coerce'
                        ).dt.strftime('%Y-%m-%d')
                    if 'fecha' in df.columns:
                        df['fecha'] = pd.to_datetime(
                            df['fecha'], errors='coerce'
                        ).dt.strftime('%Y-%m-%d')

                    # Insertar en la tabla correspondiente
                    df.to_sql(name=table_name, con=self.engine_dest, if_exists='append', index=False)
                    logging.info(f"Datos insertados en la tabla '{table_name}'.")
                else:
                    logging.info(f"La tabla '{table_name}' está vacía. No se insertaron datos.")
        except Exception as e:
            logging.error(f"Error al insertar datos en las tablas: {e}")
            raise

    def convertir_txt_a_csv(self):
        """
        Convierte el archivo 'Regions.txt' a 'Regions.csv' agregando encabezados y formateando correctamente.
        """
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
            raise

    def cargar_dimension_tiempo_ventas(self):
        """
        Carga la dimensión de tiempo y la tabla de ventas desde las tablas de origen,
        transformando y preparando los datos para su inserción.
        """
        try:
            # Usar el engine de origen para cargar las tablas
            origin_metadata = MetaData()
            origin_metadata.bind = self.engine_origin
            billing = Table('billing', origin_metadata, autoload_with=self.engine_origin)
            billing_detail = Table('billing_detail', origin_metadata, autoload_with=self.engine_origin)
            prices = Table('prices', origin_metadata, autoload_with=self.engine_origin)

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

            with self.engine_origin.connect() as connection:
                result_proxy = connection.execute(query)
                billing_data = result_proxy.fetchall()

            billing_df = pd.DataFrame(billing_data, columns=[
                'billing_id', 'region', 'branch_id', 'date', 'customer_id',
                'employee_id', 'product_id', 'quantity', 'date_price', 'price'
            ])
            billing_df['date'] = pd.to_datetime(billing_df['date'], errors='coerce')
            billing_df.dropna(subset=['date'], inplace=True)

            # Crear la dimensión tiempo
            unique_dates = billing_df['date'].drop_duplicates().reset_index(drop=True)
            self.dimension_tiempo = pd.DataFrame({
                'id_tiempo': unique_dates.index + 1,
                'fecha': unique_dates,
                'dia': unique_dates.dt.day,
                'mes': unique_dates.dt.month,
                'trimestre': unique_dates.dt.quarter,
                'año': unique_dates.dt.year
            })

            # Mapear las fechas a id_tiempo
            date_id_map = self.dimension_tiempo.set_index('fecha')['id_tiempo']

            # Crear la tabla de ventas
            self.ventas = pd.DataFrame({
                'id_tiempo': billing_df['date'].map(date_id_map),
                'id_cliente': billing_df['customer_id'],
                'id_vendedor': billing_df['employee_id'],
                'id_producto': billing_df['product_id'],
                'cantidad': billing_df['quantity'],
                'precio': billing_df['price']
            })

            logging.info('Tabla de ventas creada con éxito.')
        except NoSuchTableError as e:
            logging.error(f"No se encontró la tabla: {e}")
            raise
        except Exception as e:
            logging.error(f'Error al cargar la dimensión de tiempo y las ventas: {e}')
            raise

    def cargar_clientes(self):
        """
        Carga y procesa los datos de clientes minoristas y mayoristas, los combina y valida.
        """
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
            raise

    def cargar_regiones(self):
        """
        Carga los datos de regiones desde el archivo CSV generado.
        """
        try:
            self.regiones = pd.read_csv('data/Regions.csv', delimiter=',')
            logging.info('Regiones cargadas.')
        except Exception as e:
            logging.error(f'Error al procesar Regions.csv: {e}')
            raise

    def cargar_empleados(self):
        """
        Carga y procesa los datos de empleados, los traduce y valida.
        """
        try:
            self.dimension_empleados = pd.read_excel('data/Employee.xls')
            self.dimension_empleados = self.traducir_empleado(self.dimension_empleados)
            self.validar_datos(self.dimension_empleados, check_column='antiguedad')
            logging.info('Empleados cargados y validados.')
        except Exception as e:
            logging.error(f'Error al procesar Employee.xls: {e}')
            raise

    def cargar_productos(self):
        """
        Carga y procesa los datos de productos, clasificando y transformando la información necesaria.
        """
        try:
            products = pd.read_csv('data/Products.txt', delimiter='|', names=['product_id', 'detalle', 'tipoEnvase'])
            df_products = products.copy()
            df_products['product_id'] = df_products['product_id'].astype(str).str.rstrip().astype(int)
            df_products['detalle'] = df_products['detalle'].str.rstrip()
            df_products['old_package'] = df_products['tipoEnvase']
            df_products['tipoEnvase'] = np.where(
                df_products['tipoEnvase'].str.contains('can', case=False), 'Can', 'Bottle'
            )

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
            raise

    def guardar_datos(self):
        """
        Guarda los DataFrames procesados en archivos CSV.
        """
        self.guardar_csv(self.dimension_clientes, 'dimension_clientes.csv')
        self.guardar_csv(self.dimension_empleados, 'dimension_vendedores.csv')
        self.guardar_csv(self.dimension_productos, 'dimension_productos.csv')
        self.guardar_csv(self.dimension_tiempo, 'dimension_tiempo.csv')
        self.guardar_csv(self.ventas, 'tabla_hecho_ventas.csv')

    @staticmethod
    def guardar_csv(df, filename):
        """
        Guarda un DataFrame en un archivo CSV, manejando la existencia previa del archivo.

        Args:
            df (pd.DataFrame): DataFrame a guardar.
            filename (str): Nombre del archivo CSV.
        """
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
        """
        Valida si un DataFrame contiene valores nulos y muestra información relevante.

        Args:
            df (pd.DataFrame): DataFrame a validar.
            check_column (str, optional): Columna específica para verificar valores nulos.
        """
        if df.isnull().sum().any():
            logging.warning("Hay valores nulos en las siguientes columnas:")
            logging.warning(df.isnull().sum())
            if check_column and df[check_column].isnull().sum() > 0:
                logging.warning(f"Valores nulos en la columna '{check_column}':")
                logging.warning(df[df[check_column].isnull()])
        else:
            logging.info("No hay valores nulos en los datos.")

    def traducir_cliente(self, df, tipo_cliente):
        """
        Traduce y procesa los datos de clientes, agregando información de zona y rango de edad.

        Args:
            df (pd.DataFrame): DataFrame de clientes.
            tipo_cliente (str): Tipo de cliente ('minorista' o 'mayorista').

        Returns:
            pd.DataFrame: DataFrame procesado y traducido.
        """
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
            region = self.regiones.loc[
                (self.regiones['CITY'] == ciudad) & (self.regiones['STATE'] == estado)
            ]
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
        """
        Calcula la edad basada en la fecha de nacimiento.

        Args:
            fecha_nacimiento (datetime): Fecha de nacimiento.

        Returns:
            int: Edad calculada.
        """
        if pd.isnull(fecha_nacimiento):
            return None
        today = datetime.today()
        return today.year - fecha_nacimiento.year - (
            (today.month, today.day) < (fecha_nacimiento.month, fecha_nacimiento.day)
        )

    @staticmethod
    def traducir_empleado(df):
        """
        Traduce y procesa los datos de empleados, calculando antigüedad y asignando zonas.

        Args:
            df (pd.DataFrame): DataFrame de empleados.

        Returns:
            pd.DataFrame: DataFrame procesado y traducido.
        """
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
        df['zona'] = np.where(
            df['antiguedad'] < 1, 'Nuevo',
            np.where(df['antiguedad'] < 5, 'Medio', 'Antiguo')
        )

        return df[['id_vendedor', 'nombre', 'sexo', 'antiguedad', 'fecha_nacimiento', 'zona']]

    def crear_dump_mysqldump(self, output_file='dump.sql'):
        """
        Crea un dump de la base de datos destino utilizando el comando `mysqldump`.

        Args:
            output_file (str): Nombre del archivo de salida donde se guardará el dump.
        """
        try:
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_host = os.getenv('DB_HOST')
            db_port = os.getenv('DB_PORT')
            db_name = self.db_name_dest

            dump_cmd = [
                'mysqldump',
                f'--user={db_user}',
                f'--password={db_password}',
                f'--host={db_host}',
                f'--port={db_port}',
                db_name
            ]

            with open(output_file, 'w') as f:
                subprocess.run(dump_cmd, stdout=f, check=True)

            logging.info(f"Dump de la base de datos '{db_name}' creado exitosamente en '{output_file}'.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al ejecutar mysqldump: {e}")
            raise
        except Exception as e:
            logging.error(f"Error al crear el dump de la base de datos: {e}")
            raise


if __name__ == "__main__":
    etl = ETLProcess()
    etl.cargar_clientes()
    etl.cargar_empleados()
    etl.cargar_productos()
    etl.cargar_dimension_tiempo_ventas()
    etl.guardar_datos()
    etl.generar_esquema_sql()
    etl.insertar_datos()
    etl.crear_dump_mysqldump('etl_dump.sql')
    logging.info("Proceso ETL completado con éxito.")
