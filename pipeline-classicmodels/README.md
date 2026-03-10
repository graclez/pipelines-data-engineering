# Proyecto dbt – ClassicModels

## Descripción del Proyecto

Este repositorio contiene un proyecto desarrollado con **dbt (Data Build Tool)** utilizando la base de datos **ClassicModels**. El objetivo del proyecto es transformar datos operacionales en modelos analíticos organizados que permitan facilitar el análisis de información relacionada con clientes, productos, pedidos y ventas.

El proyecto aplica buenas prácticas de ingeniería de datos mediante la construcción de modelos modulares, organizados en diferentes capas de transformación. Estas capas permiten limpiar, transformar y estructurar los datos de manera progresiva hasta generar datasets optimizados para análisis y reporting.

Además, se incorporan mecanismos de control de calidad de datos mediante tests, así como documentación automática de los modelos para facilitar la comprensión del flujo de datos y las dependencias entre tablas.

---

## Arquitectura del Proyecto

El flujo de transformación de datos sigue una arquitectura en capas que permite separar responsabilidades y mejorar la mantenibilidad del pipeline de datos.

Los datos provienen inicialmente de la base de datos ClassicModels, que contiene información transaccional. Posteriormente, estos datos son transformados mediante diferentes modelos dbt hasta obtener estructuras optimizadas para análisis.

Las capas del proyecto incluyen:

* **Staging:** limpieza, estandarización y preparación inicial de los datos provenientes de las tablas fuente.
* **Intermediate:** integración y enriquecimiento de datos mediante joins y transformaciones adicionales.
* **Mart:** construcción de modelos analíticos finales que contienen métricas y dimensiones listas para consumo por herramientas de análisis o visualización.

Esta organización permite mantener un flujo de transformación claro, escalable y fácil de mantener.

---

## Estructura del Repositorio

El proyecto se encuentra organizado siguiendo la estructura recomendada por dbt. Los modelos se encuentran dentro de la carpeta `models`, separados por capas de transformación. También se incluyen directorios para pruebas, seeds y snapshots, que permiten ampliar la funcionalidad del pipeline de datos.

Dentro del proyecto se encuentran archivos de configuración que permiten definir el comportamiento de dbt, así como la conexión con la base de datos y las dependencias entre modelos.

Esta organización facilita la reutilización de modelos, el mantenimiento del código y la comprensión del flujo de transformación de datos.

---

## Modelado de Datos

Los modelos desarrollados en este proyecto permiten transformar los datos operacionales de ClassicModels en estructuras analíticas que facilitan el análisis del negocio.

Las tablas finales permiten analizar información relacionada con:

* comportamiento de los clientes
* ventas realizadas
* productos comercializados
* relación entre pedidos, clientes y productos

Las transformaciones realizadas incluyen limpieza de datos, estandarización de nombres de columnas, integración de múltiples tablas y cálculo de métricas relevantes para análisis.

El resultado final del proyecto es un conjunto de modelos analíticos que permiten explorar la información de forma estructurada y eficiente.

---

## Calidad de Datos

Para garantizar la confiabilidad de la información generada, el proyecto incorpora pruebas de calidad de datos utilizando las capacidades nativas de dbt. Estas pruebas permiten verificar que los datos cumplan ciertas reglas antes de ser utilizados en análisis.

Entre las validaciones aplicadas se encuentran controles de unicidad, validación de valores nulos en campos clave y verificación de relaciones entre tablas.

Este enfoque permite detectar posibles problemas en los datos de forma temprana y asegurar la integridad del pipeline de transformación.

---

## Documentación del Proyecto

El proyecto utiliza las funcionalidades de documentación de dbt para describir los modelos y su relación dentro del pipeline de datos. La documentación generada permite visualizar las dependencias entre modelos y comprender el flujo de transformación desde las tablas fuente hasta los modelos analíticos finales.

Esto facilita el mantenimiento del proyecto y permite que otros miembros del equipo comprendan rápidamente la estructura y funcionamiento del pipeline de datos.

---

## Tecnologías Utilizadas

El desarrollo de este proyecto utiliza las siguientes herramientas y tecnologías:

* dbt (Data Build Tool)
* SQL
* Git
* GitHub
* Base de datos ClassicModels

Estas herramientas permiten construir pipelines de transformación reproducibles, versionados y documentados.

---

## Objetivo del Proyecto

El objetivo principal del proyecto es demostrar el uso de dbt para la construcción de pipelines de transformación de datos organizados, reutilizables y orientados al análisis.

Mediante este proyecto se busca aplicar buenas prácticas de ingeniería de datos, incluyendo modelado modular, control de calidad de datos, documentación del pipeline y uso de control de versiones.

---

## Autor

Graciela Lezcano

