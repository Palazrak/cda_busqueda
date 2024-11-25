
# Centro de Datos, Algoritmos y Sociedad ITAM

# Proyecto para colectivos de búsqueda

## 1. Introducción

Este proyecto está enfocado a colectivos de búsqueda a lo largo del país. Consiste en la implementación de una app web con la cual se podrán realizar búsquedas de expedientes de desaparecidos de las distintas fiscalías de México.

El sistema consiste en un Docker Compose, en donde la estructura general del proyecto consiste en:

- Una base de datos Postgres, en la cual se guardará la información de las fichas
- Scrappers en Python utilizados para extraer información de las fiscalías e ingresarlos a la base de datos
- Una API REST montada con PostgREST, para manejar las acciones de consulta a la base de datos
- Una app web, con la cual podrá usarse el servicio completo

### 1.1 Base de datos Postgres

Se decidió utilizar Postgres por la funcionalidad de búsqueda fonética que tiene implementada de forma nativa con el paquete `fuxxystrmatch`. Esta funcionalidad será útil para ser robustos ante errores ortográficos ya sea en la búsqueda de expedientes o por si hubo algún error al capturar la información a la hora de crear las fichas.

Dentro de la base de datos, se tiene una tabla con la siguiente estructura:

- **id** (SERIAL PRIMARY KEY): id de la inserción, siendo un entero secuencial.
- **fecha_extraccion** (DATE): Fecha en la cual se extrajo la ficha.
- **url_origen** (TEXT): URL del cual proviene la ficha.
- **fecha_modificacion** (TIMESTAMP): Fecha de la última actualización (por ejemplo, si una persona fue localizada, la fecha en la cual se generó esa actualización).
- **localizado** (BOOLEAN): Indica si la persona ha sido reportada como localizada.
- **datos** (JSONB): Información de la persona en formato jsonb. Dependiendo de la fiscalía, los campos que contiene podrían variar.

Fue relevante utilizar JSONB para aprovechar la ventaja de la búsqueda fonética mientras que aprovechamos la flexibilidad que ofrecen los json. Ya que las fiscalías son estatales y no tienen un formato generalizado, es necesario permitir esa flexibilidad.

# 1.2 API PostgREST

La API que decidimos utilizar para hacer consultas a la base de datos es PostgREST. Está montada en el puerto 3000.

# 1.3 Frontend: NodeJS

La página interactiva que se puede utilizar para hacer las operaciones de búsqueda (y eventualmente de inserción) a la base de datos está ubicada en el puerto 3001 y fue hecha con NodeJS y diseñada para ser Responsive.

## 2. Inicialización del proyecto

Una vez descargado el repo, es necesario un .env con ciertas credenciales para que ejecute inmediatamente. A la altura del repositorio, en la terminal, es necesario ejecutar el siguiente comando:

```bash
docker compose up -d
```

Posteriormente, empezarán a correr todos los contenedores. El contenedor `python_scrapper` tarda bastante en ejecutar, ya que scrappea los elementos de 95 pestañas dentro de la pagina de [HAVISTOA Chiapas](https://www.fge.chiapas.gob.mx/servicios/hasvistoa). Para revisar el estado del contenedor pueden verse los logs del contenedor.

Al finalizar el contenedor Python Scrapper, se puede ingresar a la base de datos Postgres con el siguiente comando:

```bash
docker exec -it postgres psql -U postgres -d cda_busqueda  
```

Dentro del contenedor, pueden hacerse consultas sobre la tabla `desaparecidos`. Otra alternativa es que, una vez que se haya hecho la inserción, puede probarse el correcto funcionamiento de la API ejecutando el siguiente comando en la terminal:

```bash
curl http://localhost:3000/desaparecidos
```