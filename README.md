
# TP4-SDG

Este proyecto consiste en la creación y manipulación de una base de datos MySQL para gestionar información de ventas. Incluye la creación de la base de datos, restauración de un backup y ejecución de un proceso de ETL (Extract, Transform, Load) mediante Jupyter Notebook.

## Requisitos previos

1. Tener instalado **MySQL**.
2. Tener instalado **Python** con las siguientes bibliotecas:
   - `pandas`
   - `mysql-connector-python`
   - `SQLAlchemy`
   - `Jupyter Notebook`

3. Clonar este repositorio o tener los archivos **.env**, **backup.sql**, y **ETL.ipynb** disponibles.

### Configuración del archivo `.env`

El archivo `.env` contiene las credenciales de acceso a la base de datos. Asegúrate de que esté configurado correctamente antes de continuar.

```ini
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=.
DB_NAME=sales
DB_PORT=3306
```

## Instrucciones

### 1) Conexión a la base de datos

Para conectarte a la base de datos MySQL, asegúrate de que el servicio MySQL esté corriendo en tu máquina. Luego, sigue estos pasos:

1. Abre una terminal.
2. Conéctate a MySQL usando el siguiente comando (ajusta las credenciales según sea necesario):

```bash
mysql -u root -p
```

Cuando te lo solicite, ingresa la contraseña configurada en tu archivo `.env`.

### 2) Creación de la base de datos

Una vez conectado a MySQL, ejecuta el siguiente comando para crear la base de datos:

```sql
CREATE DATABASE sales;
```

### 3) Restauración del backup

Si ya tienes un archivo de backup (`backup.sql`), puedes restaurar la base de datos utilizando el siguiente comando:

```bash
mysql -u root -p sales < backup.sql
```

Este comando restaurará el contenido del archivo `backup.sql` en la base de datos que acabas de crear.

### 4) Ejecución del ETL

El proceso ETL (Extracción, Transformación, Carga) se ejecuta a través del archivo **ETL.ipynb**.

#### Pasos para ejecutar el ETL:

1. Abre Jupyter Notebook en el directorio donde se encuentra el archivo **ETL.ipynb**:

```bash
jupyter notebook
```

2. Dentro de Jupyter, abre el archivo **ETL.ipynb**.
3. Ejecuta cada celda secuencialmente para completar el proceso de ETL.

Este proceso extraerá datos de la base de datos MySQL, los transformará según sea necesario y luego los cargará en la base de datos o en otros destinos según esté configurado el archivo ETL.

---

### Notas

- Asegúrate de que las credenciales del archivo `.env` coincidan con la configuración de tu MySQL.
- Si experimentas problemas con la restauración del backup, asegúrate de que el archivo `backup.sql` esté en el directorio correcto y que la base de datos esté vacía antes de restaurar.
- Si encuentras algún error al ejecutar el notebook ETL, asegúrate de tener las bibliotecas de Python correctas instaladas.

---
