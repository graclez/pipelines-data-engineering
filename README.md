# Pipelines Data Engineering - Trabajo final

Repositorio que contiene la implementación de **pipelines analíticos end-to-end** desarrollados como parte del proyecto final de **Integración de Datos**.

El objetivo del repositorio es demostrar la construcción de una arquitectura moderna de datos utilizando herramientas ampliamente utilizadas en entornos de **Data Engineering**.

## Arquitectura del Pipeline

Los pipelines siguen una arquitectura ELT compuesta por las siguientes herramientas:

* **Airbyte** → extracción e ingesta de datos
* **MotherDuck / DuckDB** → almacenamiento analítico
* **dbt** → transformación y modelado de datos
* **Prefect** → orquestación del pipeline
* **Metabase** → visualización y dashboards

Flujo general:

Fuente de datos → Airbyte → MotherDuck → dbt → Prefect → Metabase

---

# Proyectos incluidos

Este repositorio contiene **dos pipelines independientes**, cada uno implementando un caso analítico diferente.

## 1️⃣ Pipeline dvdrental

Carpeta:

```
pipeline-dvdrental
```

Pipeline analítico basado en el dataset **dvdrental**, utilizado para analizar el negocio de alquiler de películas.

Análisis implementados:

* ingresos por alquiler
* actividad por tienda
* comportamiento de clientes
* popularidad de películas
* evolución temporal de alquileres

El pipeline incluye:

* extracción con Airbyte
* modelos dbt
* pruebas de calidad de datos
* orquestación con Prefect
* dashboard analítico en Metabase

---

## 2️⃣ Pipeline ClassicModels

Carpeta:

```
pipeline-classicmodels
```

Pipeline orientado al **análisis de ventas comerciales** utilizando el dataset ClassicModels.

Modelo analítico implementado:

* dimensión **dim_customer**
* tabla de hechos **fact_sales**

Métricas analizadas:

* ventas totales
* cantidad de órdenes
* ticket promedio
* top clientes
* productos más vendidos

El pipeline incluye:

* ingestión de datos
* modelado analítico con dbt
* validaciones de calidad
* ejecución orquestada con Prefect
* dashboard de ventas en Metabase

---

# Estructura del repositorio

```
pipelines-data-engineering
│
├── pipeline-dvdrental
│   ├── models
│   ├── macros
│   ├── tests
│   ├── dbt_project.yml
│   └── pipeline.py
│
├── pipeline-classicmodels
│   ├── models
│   ├── macros
│   ├── tests
│   ├── dbt_project.yml
│   └── pipeline.py
│
└── README.md
```

---

# Tecnologías utilizadas

* Python
* dbt
* DuckDB
* MotherDuck
* Airbyte
* Prefect
* Metabase
* Git / GitHub

---

# Ejecución del pipeline

Dentro de cada proyecto se puede ejecutar:

```
dbt run
dbt test
dbt docs generate
dbt docs serve
```

La orquestación puede ejecutarse mediante:

```
python pipeline.py
```

---

# Autores

**Graciela Lezcano** - **César Gonzalez** - 
Facultad Politécnica – Universidad Nacional de Asunción

Proyecto desarrollado como parte del curso **Introducción a la Integración de Datos**.
