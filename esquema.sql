
CREATE TABLE dimension_clientes (
	id_cliente INTEGER NOT NULL, 
	nombre VARCHAR(255), 
	ciudad VARCHAR(100), 
	provincia VARCHAR(100), 
	zona VARCHAR(100), 
	tipo_cliente VARCHAR(100), 
	rango_edad VARCHAR(50), 
	PRIMARY KEY (id_cliente)
)

;


CREATE TABLE dimension_vendedores (
	id_vendedor INTEGER NOT NULL, 
	nombre VARCHAR(255), 
	sexo VARCHAR(50), 
	antiguedad INTEGER, 
	fecha_nacimiento DATE, 
	zona VARCHAR(55), 
	PRIMARY KEY (id_vendedor)
)

;


CREATE TABLE dimension_productos (
	product_id INTEGER NOT NULL, 
	detalle VARCHAR(255), 
	`tipoEnvase` VARCHAR(50), 
	litros FLOAT, 
	`tipoBebida` VARCHAR(50), 
	PRIMARY KEY (product_id)
)

;


CREATE TABLE dimension_tiempo (
	id_tiempo INTEGER NOT NULL, 
	fecha DATE, 
	dia INTEGER, 
	mes INTEGER, 
	trimestre INTEGER, 
	`año` INTEGER, 
	PRIMARY KEY (id_tiempo)
)

;


CREATE TABLE tabla_hecho_ventas (
	id_venta INTEGER NOT NULL AUTO_INCREMENT, 
	id_tiempo INTEGER, 
	id_cliente INTEGER, 
	id_vendedor INTEGER, 
	id_producto INTEGER, 
	cantidad INTEGER, 
	precio FLOAT, 
	PRIMARY KEY (id_venta), 
	FOREIGN KEY(id_tiempo) REFERENCES dimension_tiempo (id_tiempo), 
	FOREIGN KEY(id_cliente) REFERENCES dimension_clientes (id_cliente), 
	FOREIGN KEY(id_vendedor) REFERENCES dimension_vendedores (id_vendedor), 
	FOREIGN KEY(id_producto) REFERENCES dimension_productos (product_id)
)

;

