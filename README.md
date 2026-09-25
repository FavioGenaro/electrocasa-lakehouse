# Electrocasa Lakehouse

Este proyecto implementa un lakehouse de datos para Electrocasa sobre Databricks, siguiendo una arquitectura Medalion para ingestar, validar, historizar y entregar indicadores analíticos de negocio. La solución está diseñada para ejecutarse como un bundle de Databricks, con un pipeline de ETL y una programación diaria automatizada.

La documentación se basa en la estructura real del repositorio y en el material de referencia del proyecto integrador, y contempla desde la creación de grupos y permisos hasta el despliegue a otro workspace.

---

## 1. Objetivo del proyecto

El caso de uso busca consolidar información de ventas, productos, empleados, reseñas, devoluciones y tracking de entregas en un único repositorio analítico, con capacidad para:

- Integrar fuentes heterogéneas en un catálogo unificado.
- Separar la ingesta cruda de la lógica analítica.
- Aplicar validaciones de calidad y reglas de historización en la capa silver.
- Publicar indicadores y agregados empresariales en gold.
- Entregar acceso por perfiles funcionales con controles de seguridad a nivel de catálogo y esquema.

---

## 2. Arquitectura propuesta

- Nombre del catálogo: `electrocasa`
- Esquemas: `bronze`, `silver`, `gold`

### 2.1 Origen de datos 

- Volumen de landing: `electrocasa.bronze.landing`

La solución usa una arquitectura Lakehouse con tres capas principales:

### 2.2 Capa Bronze

Dentro de `electrocasa.bronze`.

En esta capa se almacenan los datos tal como llegan desde las fuentes de origen. Se garantiza que los datos de entrada no se pierdan y se preserven para auditoría y reprocesos.

En esta capa se materializan las tablas:

- `ventas_bronze`
- `productos_bronze`
- `empleados_bronze`
- `resenas_bronze`
- `devoluciones_bronze`
- `tracking_bronze`


### 2.3 Capa Silver

Dentro de `electrocasa.silver` se aplican:

- limpieza y normalización de datos.
- validaciones con `expect_all`.
- tratamiento de tipos de datos.
- uso de historización para conservar el estado de los registros.

En esta capa se materializan las tablas:

- `ventas_silver`
- `productos_silver`
- `empleados_silver`
- `resenas_silver`
- `devoluciones_silver`
- `tracking_silver`

### 2.4 Capa Gold

En `electrocasa.gold` se publican vistas materializadas y agregaciones orientadas a analítica y reporting. Ejemplos del repositorio:

- `ventas_sucursal_mes`
- `dotacion_sucursal`
- `ranking_productos`
- `resenas_categoria`
- `resenas_categoria`
- `tracking_envios`

Estas tablas están pensadas para responder preguntas de negocio, métricas y KPIs, no para conservar copias de origen.

---

## 3. Justificación del uso de tablas y historización

Se eligieron tablas Delta por varias razones:

- Soporte nativo para ACID transactions.
- Compatibilidad con Unity Catalog.
- Mejor rendimiento para lecturas y escrituras analíticas.
- Capacidad de optimización automática con `delta.autoOptimize.optimizeWrite` y `delta.autoOptimize.autoCompact`.

### 3.1 Tipo de historización por entidad

El proyecto usa dos tipos de historización según la naturaleza de cada dato:

#### SCD Type 1

Se usa en tablas donde no interesa conservar el histórico completo del valor por cambios temporales. Esto aplica porque se prioriza el estado actual de los datos y se asume que la gran mayoría de estas entidades se consultan en su versión actual. Las tablas son:

- ventas
- productos
- reseñas
- devoluciones
- tracking


#### SCD Type 2

Se usa en `empleados` porque el dato de empleado tiene evolución temporal real: cambios de cargo, transferencia de sucursal, salario y estado asociado a un mismo identificador. En contexto laboral, es importante preservar el historial completo del empleado para análisis de dotación, evolución salarial, cambios de rol y auditoría.

Por esa razón, la metadata de `empleados` define:

- `type: 2`
- `keys`: `id_empleado`, `fecha_evento`, `tipo_evento`
- `track_history_column_list` para guardar atributos históricos

### 3.2 Tipo de tabla

- Vistas materializadas sobre `tracking` y `productos` porque el origen entrega una foto completa de los datos con alteraciones menores y de baja volumetria, siendo que volverla a escribir completa es extremadamente bajo y no toma tiempo.

- Tablas Streaming sobre `ventas`, `reseñas`, `devoluciones` y `empleados` para tablas con mayor flujo de información que requiere ser almacenada de forma incremental.

Toda las tablas, excepto `tracking`, son del tipo Managed para que Databricks ejecute de forma autónoma tareas de mantenimiento como OPTIMIZE y VACUUM, además de aplicar optimizaciones en tiempo ejecución. La tabla `tracking` proviene de una fuente externa de datos (base de datos) por lo que será una tabla tipo External

---

## 4. Estructura del repositorio

### 4.1 Configuración del bundle

Archivo principal:

- [databricks.yml](databricks.yml)

Define:

- El nombre del bundle: `electrocasa_project`
- Catálogo principal: `electrocasa`
- Esquemas: `bronze`, `silver`, `gold`
- Target `dev`
- Host del workspace destino
- Permisos del usuario actual para gestionar el bundle

### 4.2 Recursos de Databricks

Archivos en [resources](resources):

- [resources/electrocasa_pipeline.yml](resources/electrocasa_pipeline.yml): Define el declarative pipeline y su configuración de catálogo/esquema.
- [resources/electrocasa_job.yml](resources/electrocasa_job.yml): Define el job que ejecuta el pipeline y un notebook de validación.

### 4.3 Notebook de setup

Archivo:

- [notebooks/00_setup.ipynb](notebooks/00_setup.ipynb)

Este notebook realiza la creación inicial del entorno:

- Catálogo `electrocasa`
- Schemas `bronze`, `silver` y `gold`
- Volumen `electrocasa.bronze.landing`
- Asignación de permisos por grupo

### 4.4 Metadata y fuentes

Archivos:

- [metadata/sources.json](metadata/sources.json)
- [metadata/silver.json](metadata/silver.json)

Estos archivos describen:

- Nombres de las fuentes.
- Rutas de landing.
- Esquemas por tabla.
- Type de historización.
- Propiedades del Delta table.
- Validaciones de calidad.

### 4.5 Transformaciones

Carpeta:

- [src/electrocasa](src/electrocasa)

Contiene la lógica de la creación de las tablas, transformaciones y utilidades:

- `utils/ingestion.py`: Lectura de archivos, columnas de auditoría, CDC automático
- `utils/metadata.py`: Carga y acceso a metadata JSON
- `utils/schemas.py`: Conversión de tipos declarados a `StructType`
- `transformations/bronze`: Ingestión y carga a bronze
- `transformations/silver`: Limpieza, validación y SCD
- `transformations/gold`: Materialized views para KPIs

---

## 5. Pasos para la ejecución del proyecto

### 5.1 Creación de grupos

La seguridad del proyecto se gestiona con Unity Catalog y grupos a nivel de cuenta. El notebook de setup documenta explícitamente que los grupos deben crearse desde el Managed Account para que puedan asignarse los permisos sobre Unity Catalog:

- `engineering_team`
- `analysts_team`
- `audit_team`

Esto es importante porque los grupos deben existir a nivel de cuenta para que sean reconocidos correctamente por Unity Catalog en el workspace asociado.


- Login en la cuenta con permisos sobre el Managed Account: 

  `databricks auth login --host <link-host> --account-id <account-id> --profile ACCOUNT`

- Creación de grupos: 

  - `databricks account groups create --display-name engineering_team`
  - `databricks account groups create --display-name analysts_team`
  - `databricks account groups create --display-name audit_team`

### 5.2 Ejecutar notebook 00_setup

Creamos los catalogos, schemas y volumns, adicionalmente se asignan los permisos a los grupos creados en base a la siguiente lógica:

#### engineering_team

Es el equipo técnico que trabaja el pipeline y la capa operacional. Deben realizar la ingesta, validación, corrección y mantención de los datos en todas las capas.

Permisos asignados:

- `USE CATALOG` sobre `electrocasa`
- `USE SCHEMA` + `SELECT` + `MODIFY` sobre `electrocasa.bronze`
- `USE SCHEMA` + `SELECT` + `MODIFY` sobre `electrocasa.silver`
- `USE SCHEMA` + `SELECT` + `MODIFY` sobre `electrocasa.gold`

#### analysts_team

Es el perfil de negocio y analítica que necesita acceso a KPIs y tablas analíticas maduras, sin acceso a la capa operacional cruda.

Permisos asignados:

- `USE CATALOG` sobre `electrocasa`
- `USE SCHEMA` + `SELECT` sobre `electrocasa.gold`

#### audit_team

Es el equipo de auditoría y control para revisar la estructura y los resultados finales sin necesidad de manipular los datos activos.

Permisos asignados:

- `USE CATALOG` sobre `electrocasa`
- `USE SCHEMA` + `SELECT` sobre `electrocasa.gold`
- `BROWSE` sobre el catálogo.


### 5.3 Cargar datos

Dentro de volumn Langing se deben crear las siguientes carpetas y carga los datos indicados:

- /devoluciones
- /empleados
- /metadata
  - /bronze: sources.json
  - /silver: silver.json
- /productos
- /resenas
- /ventas

Los datos de tracking serán consumidos desde una base de datos, por lo que se debe configuración la conexión correspondientes dentro del archivo [metadata/sources.json](metadata/sources.json), así como la configuración de los secrets con las siguientes denominaciones:

- scope-secret: electrocasa
- key user: user_db
- key password: password_db

### 5.4. Deploy

#### Configuración del host y variables

El archivo [databricks.yml](databricks.yml) define el host del workspace actual:

```yaml
workspace:
  host: https://adb-7405615539762593.13.azuredatabricks.net/
```

Cuando se despliega en otro workspace se debe actualizar este valor y, si es necesario, el catálogo por defecto o los esquemas.

Ejemplo:

```yaml
variables:
  catalog:
    default: electrocasa
  schema_bronze:
    default: bronze
  schema_silver:
    default: silver
  schema_gold:
    default: gold
```

#### Validación del bundle

Desde la raíz del proyecto:

```bash
databricks bundle validate
```

Esto valida sintaxis y configuración del bundle antes de desplegar.

#### Despliegue

Ejecuta:

```bash
databricks bundle deploy --target dev
```

Esto despliega:

- El lakeflow declarative pipeline en [resources/electrocasa_pipeline.yml](resources/electrocasa_pipeline.yml).
- El job definido en [resources/electrocasa_job.yml](resources/electrocasa_job.yml).
- La estructura del proyecto en el workspace destino.

![Imagen del despligue de los recursos](/capturas/deploy.png)

#### Ejecución

Ejecuta:

```bash
databricks bundle run
```

Selecciona entre ejecutar el Job o el Pipeline desplegado.

![Imagen de los comandos ejecutados](/capturas/comandos.png)

Resultados de la ejecución del Job según la estructura definida:

![Imagen del job ejecutado](/capturas/job_ui.png)

Resultados de la ejecución del Pipeline:

![Imagen del pipeline ejecutado](/capturas/pipeline.png)

Resultados de la ejecución del notebook:

![Imagen del notebook ejecutado](/capturas/notebook.png)

Notificacón configurada:

![Imagen del notebook ejecutado](/capturas/notificacion.png)

Estructura del catalogo creado:

![Imagen del catalogo creado](/capturas/catalogo.png)


Adicionalmente, se integro al job el notebook [01_validaciones](/notebooks/01_validaciones.ipynb) que consulta a las tablas creadas por el pipeline, así como, consultas para el monitoreo de la ejecución.

---

## 7. programación

La programación está configurada en Quartz con:

```yaml
quartz_cron_expression: 15 59 23 * * ?
timezone_id: America/Lima
pause_status: UNPAUSED
```

Esto corresponde a una ejecución diaria a las 23:59:00 hora de Lima, es decir, una vez al día.

Se estableció una frecuencia diaria porque el caso de uso se comporta como un ETL de carga regular, considerando que la frecuencia de actualización de las tablas en su mayoria es diaria y de baja frecuencia, y no requiere ejecución en tiempo real ni continua.

---

## 8. Troubleshooting

Se cuenta con una Event log para monitorear el estado de las ejecuciones del job y pipeline. Algunas consultas de monitoreo se ubican en el archivo [01_validaciones](/notebooks/01_validaciones.ipynb).

![Imagen del montoreo](/capturas/monitoreo.png)

## 8. Suposiciones de cluster y costos

### 8.1 Tipo de cluster asumido

La solución usa un pipeline `serverless` y un job con queue enabled. Esto implica:

- No requiere crear un clúster permanente para la ejecución del pipeline.
- Se paga por los recursos realmente utilizados en cada ejecución.
- Se reduce la sobrecarga operativa en comparación con un cluster dedicado.

También se usa `performance_target: PERFORMANCE_OPTIMIZED`, que busca equilibrar latencia y rendimiento para las cargas analíticas y tareas de transformación.

### 8.2 Costo asumido

La estrategia de costo asumida es:

- Uso serverless para cargas diarias y transformaciones moderadas.
- Ejecución automática una vez al día con un tiempo aproximando de ejecución de 3 a 4 minutos.
- No se mantienen clústeres siempre encendidos.
- Arquitectura orientada a volumen de datos medio o pequeño-mediano.

Bajo esta configuración, el costo esperado es relativamente bajo para un entorno de prueba con carga moderada. Si el volumen crece o la latencia debe disminuir, se podría pasar a clústeres dedicados o ajustar el pipeline para parallelismo más agresivo.

- Cluster Serverless 0,550 US$ Por DBU por hora, considerando una hora diara a lo mucho, serian unos 16.5 US$.
- Costo de la infraestructura total según la sección de facturación de Azure por dia se estima 1.2 US$, por lo que al mes resulta 1.2 x 30 = 36 US$ en total.

---

## 10. Consideraciones de seguridad y gobernanza

La solución usa Unity Catalog para garantizar:

- Separación de responsabilidades por capa.
- Control de acceso por catálogo, esquema y grupo.
- Trazabilidad sobre los datos de negocio.
- Aislamiento entre los perfiles de ingeniería, analítica y auditoría.

Esto es clave para un entorno de datos empresarial, ya que evita que usuarios no autorizados puedan consultar o alterar datos sensibles o operativos.

---

