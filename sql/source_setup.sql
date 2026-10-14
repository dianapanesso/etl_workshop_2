USE grammy_db;

-- Crear la tabla cruda con columnas de texto amplio para evitar errores de longitud
CREATE TABLE grammy_awards (
    year VARCHAR(255),
    title TEXT,
    published_at VARCHAR(255),
    updated_at VARCHAR(255),
    category TEXT,
    nominee TEXT,
    artist TEXT,
    workers TEXT,
    img TEXT,
    winner VARCHAR(50)
);

-- Cargar los datos del CSV a la tabla (ignora la primera fila de encabezados)
LOAD DATA INFILE '/var/lib/mysql-files/the_grammy_awards.csv'
INTO TABLE grammy_awards
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;