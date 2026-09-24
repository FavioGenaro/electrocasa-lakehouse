# Electrocasa Lakehouse

Este proyecto implementa un lakehouse de datos para Electrocasa sobre Databricks, siguiendo una arquitectura Medalion para ingestar, validar, historizar y entregar indicadores analíticos de negocio. La solución está diseñada para ejecutarse como un bundle de Databricks, con un pipeline de ETL y una programación diaria automatizada.

La documentación se basa en la estructura real del repositorio y en el material de referencia del proyecto integrador, y contempla desde la creación de grupos y permisos hasta el despliegue a otro workspace.

---

## 1. Objetivo del proyecto

El caso de uso busca consolidar información de ventas, productos, empleados, reseñas, devoluciones y tracking de entregas en un único repositorio analítico, con capacidad para:

- Integrar fuentes heterogéneas en un catálogo unificado;
- Separar la ingesta cruda de la lógica analítica;
- Aplicar validaciones de calidad y reglas de historización en la capa silver;
- Publicar indicadores y agregados empresariales en gold;
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

- limpieza y normalización de datos;
- validaciones con `expect_all`;
- tratamiento de tipos de datos;
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

- soporte nativo para ACID transactions;
- compatibilidad con Unity Catalog;
- mejor rendimiento para lecturas y escrituras analíticas;
- capacidad de optimización automática con `delta.autoOptimize.optimizeWrite` y `delta.autoOptimize.autoCompact`;
- idoneidad para pipelines de ingestion y reporting en Databricks.

### 3.1 Tipo de historización por entidad

El proyecto usa dos tipos de historización según la naturaleza de cada dato:

#### SCD Type 1

Se usa en tablas donde el dato más reciente reemplaza al anterior, y no interesa conservar el histórico completo del valor por cambios temporales. Ejemplos:

- ventas
- productos
- reseñas
- devoluciones
- tracking

Esto aplica porque se prioriza el estado actual del registro y se asume que la gran mayoría de estas entidades se consultan en su versión vigente.

#### SCD Type 2

Se usa en `empleados` porque el dato de empleado tiene evolución temporal real: cambios de cargo, transferencia de sucursal, salario y estado asociado a un mismo identificador. En contexto laboral, es importante preservar el historial completo del empleado para análisis de dotación, evolución salarial, cambios de rol y auditoría.

Por esa razón, la metadata de `empleados` define:

- `type: 2`
- `keys`: `id_empleado`, `fecha_evento`, `tipo_evento`
- `track_history_column_list` para guardar atributos históricos

Esto permite responder preguntas como:

- ¿cuánto ganaba un empleado antes de un cambio de salario?
- ¿cómo evolucionó su cargo a lo largo del tiempo?
- ¿qué estado tenía en una fecha puntual?

---

## 4. Estructura del repositorio

### 4.1 Configuración del bundle

Archivo principal:

- [databricks.yml](databricks.yml)

Define:

- el nombre del bundle: `electrocasa_project`
- catálogo principal: `electrocasa`
- esquemas: `bronze`, `silver`, `gold`
- target `dev`
- host del workspace destino
- permisos del usuario actual para gestionar el bundle

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

- Nombres de las fuentes;
- Rutas de landing;
- Esquemas por tabla;
- Type de historización;
- Propiedades del Delta table;
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

Es el equipo técnico que trabaja el pipeline y la capa operacional.

Permisos asignados:

- `USE CATALOG` sobre `electrocasa`
- `USE SCHEMA` + `SELECT` + `MODIFY` sobre `electrocasa.bronze`
- `USE SCHEMA` + `SELECT` + `MODIFY` sobre `electrocasa.silver`
- `USE SCHEMA` + `SELECT` + `MODIFY` sobre `electrocasa.gold`

Objetivo: permitir ingesta, validación, corrección y mantención de los datos en todas las capas.

#### analysts_team

Es el perfil de negocio y analítica.

Permisos asignados:

- `USE CATALOG` sobre `electrocasa`
- `USE SCHEMA` + `SELECT` sobre `electrocasa.gold`

Objetivo: permitir acceso a KPIs y tablas analíticas maduras, sin acceso a la capa operacional cruda.

#### audit_team

Es el equipo de auditoría y control.

Permisos asignados:

- `USE CATALOG` sobre `electrocasa`
- `USE SCHEMA` + `SELECT` sobre `electrocasa.gold`
- `BROWSE` sobre el catálogo

Objetivo: revisar la estructura y los resultados finales sin necesidad de manipular los datos activos.

---

## 6. Cómo desplegar a otro workspace

El proyecto está preparado para ser desplegado con Databricks Asset Bundles.

### 6.1 Prerrequisitos

Antes del despliegue debes tener:

- acceso administrativo al workspace destino;
- acceso de cuenta para crear o validar Unity Catalog objects;
- Databricks CLI instalado y autenticado;
- permisos para crear Jobs, Pipelines y Catálogos;
- los mismos grupos `engineering_team`, `analysts_team` y `audit_team` creados en el tenant/account asociado.

### 6.2 Configuración del host y variables

El archivo [databricks.yml](databricks.yml) define el host del workspace actual:

```yaml
workspace:
  host: https://dbc-fcc8b0b1-83a7.cloud.databricks.com
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

### 6.3 Validación del bundle

Desde la raíz del proyecto:

```bash
databricks bundle validate
```

Esto valida sintaxis y configuración del bundle antes de desplegar.

### 6.4 Despliegue

Ejecuta:

```bash
databricks bundle deploy --target dev
```

Esto despliega:

- los recursos definidos en [resources/electrocasa_pipeline.yml](resources/electrocasa_pipeline.yml)
- el job definido en [resources/electrocasa_job.yml](resources/electrocasa_job.yml)
- la estructura del proyecto en el workspace destino

### 6.5 Ajustes necesarios en un nuevo workspace

En un workspace nuevo, además de cambiar el host, es recomendable:

1. verificar que el usuario actual tenga permisos de gestión del bundle;
2. confirmar que existe el catálogo `electrocasa` o que el bundle pueda crearlo con la cuenta correcta;
3. garantizar que los grupos de seguridad ya existan en cuenta;
4. revisar si la ruta de archivos y volúmenes de landing coincide con el objetivo deseado.

### 6.6 Recomendación de uso

El proyecto se asume como un entorno de desarrollo o staging con target `dev`, aunque puede replicarse a `prod` modificando el target y ajustando permisos y variables.

---

## 7. Pipeline y programación

El pipeline se define en [resources/electrocasa_pipeline.yml](resources/electrocasa_pipeline.yml) y se configura como `serverless: true`.

El job se define en [resources/electrocasa_job.yml](resources/electrocasa_job.yml), con la siguiente lógica:

1. ejecutar notebook `00_setup`
2. esperar su finalización;
3. lanzar el pipeline `project_electrocasa_etl`

### 7.1 Programación

La programación está configurada en Quartz con:

```yaml
quartz_cron_expression: 10 0 22 * * ?
timezone_id: America/Lima
pause_status: UNPAUSED
```

Esto corresponde a una ejecución diaria a las 22:00:10 hora de Lima, es decir, una vez al día.

Se estableció una frecuencia diaria porque el caso de uso se comporta como un ETL de carga regular y consolidación reportable, y no requiere ejecución en tiempo real ni continua.

---

## 8. Suposiciones de cluster y costos

### 8.1 Tipo de cluster asumido

La solución usa un pipeline `serverless` y un job con queue enabled. Esto implica:

- no requiere crear un clúster permanente para la ejecución del pipeline;
- se paga por los recursos realmente utilizados en cada ejecución;
- se reduce la sobrecarga operativa en comparación con un cluster dedicado.

También se usa `performance_target: PERFORMANCE_OPTIMIZED`, que busca equilibrar latencia y rendimiento para las cargas analíticas y tareas de transformación.

### 8.2 Costo asumido

La estrategia de costo asumida es:

- uso serverless para cargas diarias y transformaciones moderadas;
- ejecución automática una vez al día;
- no se mantienen clústeres siempre encendidos;
- arquitectura orientada a volumen de datos medio o pequeño-mediano.

Bajo esta configuración, el costo esperado es relativamente bajo para un entorno de laboratorio, prueba o negocio con carga moderada. Si el volumen crece o la latencia debe disminuir, se podría pasar a clústeres dedicados o ajustar el pipeline para parallelismo más agresivo.

---

## 9. Flujo funcional del proyecto

El flujo actual del repositorio es:

1. Subir archivos al volumen `electrocasa.bronze.landing`.
2. Ingestar datos desde landing hacia tablas bronze.
3. Aplicar validaciones y transformaciones en silver.
4. Realizar historización mediante SCD1/SCD2 según cada tabla.
5. Generar agregados y KPIs en gold.
6. Exponer los resultados para analistas y auditoría según permisos.

---

## 10. Consideraciones de seguridad y gobernanza

La solución usa Unity Catalog para garantizar:

- separación de responsabilidades por capa;
- control de acceso por catálogo, esquema y grupo;
- trazabilidad sobre los datos de negocio;
- aislamiento entre los perfiles de ingeniería, analítica y auditoría.

Esto es clave para un entorno de datos empresarial, ya que evita que usuarios no autorizados puedan consultar o alterar datos sensibles o operativos.

---

## 11. Resumen ejecutable

Para poner en marcha el proyecto en un workspace nuevo:

1. Crear los grupos `engineering_team`, `analysts_team` y `audit_team` en Account Console.
2. Ejecutar el notebook de setup para crear catalog, schemas y volumen.
3. Asignar permisos a cada grupo con los SQL de grant mostrados en el notebook.
4. Configurar el host del workspace en [databricks.yml](databricks.yml).
5. Validar el bundle:

```bash
databricks bundle validate
```

6. Desplegar:

```bash
databricks bundle deploy --target dev
```

7. Confirmar que el job se ejecuta diariamente a las 22:00:10 (America/Lima).

---

## 12. Conclusión

El proyecto representa una implementación realista de un lakehouse empresarial en Databricks con enfoque en gobernanza, calidad y analítica de negocio. La combinación de bronze, silver y gold, la historización por tipo de entidad y la seguridad basada en grupos de Unity Catalog convierten esta solución en una base sólida para expandir analítica y reporting en Electrocasa.

La decisión de usar tablas Delta, historización SCD1/SCD2 según el caso, y un pipeline serverless con programación diaria responde a un equilibrio entre costo, cumplimiento y simplicidad operativa, manteniendo un diseño escalable para crecimiento futuro.
