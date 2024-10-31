Assessment para Data Engineer

Descripción

Este proyecto consiste en la construcción de un pipeline ETL (Extracción, Transformación y Carga) que extrae datos de la API de TVMaze, realiza una serie de transformaciones y análisis sobre los datos, y los almacena en diferentes formatos y una base de datos local para consultas avanzadas. Al final, se genera un reporte de análisis de datos y se documenta todo el proceso.

Estructura del Proyecto
data/: Contiene los datos en formato JSON y Parquet.
src/: Código fuente del proyecto.
api/: Scripts para realizar llamadas a la API de TVMaze.
transform/: Scripts para transformar los datos en DataFrames.
database/: Scripts para crear y manejar la base de datos SQLite.
analysis/: Scripts para realizar el análisis y generar reportes.
tests/: Pruebas unitarias.
notebooks/: Notebooks para pruebas y análisis de datos.
docs/: Documentación del proceso.
README.md: Información general del proyecto.

Requerimientos
Python 3.8 o superior
Paquetes necesarios especificados en requirements.txt

Instalación

Instalar las dependencias:

    1- Instalar modulos y librerias requeridas
        py -m pip install requests, pandas, pandas-profiling, fastparquet


Ejecucion

    py src/main.py

Uso del Proyecto
1. Configuración y Extracción de Datos
El proyecto comienza con la configuración del entorno y la creación de la estructura de carpetas. Luego, se realizan las llamadas a la API de TVMaze para obtener datos de series de televisión. Los datos obtenidos se guardan en archivos JSON.

2. Transformación y Análisis
Una vez que los datos están extraídos, se realizan transformaciones usando pandas para organizar y limpiar los datos. Estos se convierten en DataFrames, se procesan para manejar datos anidados, y se almacenan en archivos Parquet optimizados. Se generan reportes de perfilado con pandas-profiling y se documentan los hallazgos.

3. Carga en Base de Datos
Los datos transformados se cargan en una base de datos SQLite. La estructura de las tablas se define para reflejar las relaciones entre diferentes entidades de los datos de TVMaze, como series, episodios y actores.

4. Agregaciones y Consultas
Se realizan consultas SQL para obtener métricas específicas y se documentan los resultados y análisis.

5. Testing y Documentación Final
Se escriben pruebas unitarias para asegurar que los procesos funcionen correctamente. Finalmente, se completa la documentación del proyecto y se hace una revisión completa para la entrega.

Estructura del Código
Configuración inicial: Scripts de configuración del entorno y dependencias.
Extracción: Scripts para llamadas a la API y manejo de datos en JSON.
Transformación: Scripts para transformar datos en DataFrames y guardarlos en formato Parquet.
Carga: Scripts para almacenar datos en la base de datos SQLite.
Análisis: Scripts para generar reportes y consultas SQL para análisis.
Pruebas: Pruebas unitarias de cada etapa del pipeline.