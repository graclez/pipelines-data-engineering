# TP Final — Data Transformation with dbt (dvdrental)

## Overview

Source  
↓  
stg_*  
↓  
int_*  
↓  
dim_*  
↓  
fct_*  
↓  
obt_*  
=======
This project implements a modern **ELT analytics pipeline** using **dbt** on top of the classic PostgreSQL sample database **dvdrental**.

The objective is to demonstrate how raw operational data can be transformed into analytical datasets following two different modeling approaches:

## Modelo principal
- fct_payments 
- obt_rentals
=======
- **Dimensional modeling (Kimball / Star Schema)**
- **One Big Table (OBT)**

The pipeline follows a layered architecture where data is ingested into a **raw layer**, cleaned in **staging models**, and transformed into **analytics-ready datasets**.

---

## Project Architecture

The project follows a modern **ELT architecture** where raw data is first ingested and stored, and transformations are applied afterwards using dbt.


                ┌─────────────────────────────┐
                │      Source System          │
                │   PostgreSQL dvdrental      │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │      Ingestion Layer        │
                │          Airbyte            │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │         Raw Layer           │
                │       raw schema            │
                │  Replicated source tables   │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │       Staging Layer         │
                │         stg_* models        │
                │ - rename columns            │
                │ - type casting              │
                │ - standardization           │
                └──────────────┬──────────────┘
                               │
             ┌─────────────────┴─────────────────┐
             │                                   │
             ▼                                   ▼
┌─────────────────────────────┐     ┌─────────────────────────────┐
│   Dimensional Model         │     │      One Big Table          │
│      (Kimball)              │     │          (OBT)              │
│                             │     │                             │
│ dim_customer                │     │ obt_rentals                 │
│ dim_film                    │     │ obt_film_performance        │
│ dim_store                   │     │ obt_customer_value          │
│ dim_staff                   │     │                             │
│ dim_date                    │     └─────────────────────────────┘
│ dim_category                │
│ dim_actor                   │
│ fct_rental                  │
│ fct_payment                 │
│ bridge_*                    │
└──────────────┬──────────────┘
               │
               ▼
    ┌─────────────────────────────┐
    │      Analytics Layer        │
    │       analytics schema      │
    │  Reporting / BI / Analysis  │
    └─────────────────────────────┘

## Data Source

The project uses the **dvdrental** dataset, a sample PostgreSQL database representing a DVD rental store.

Main entities include:

- Customers
- Rentals
- Payments
- Films
- Stores
- Staff
- Actors
- Film categories

Source tables are ingested into the **raw schema**.
---

## Modeling Layers

### 1-Staging Layer

Staging models standardize the raw data and prepare it for analytical modeling.

Examples:
stg_customer
stg_payment
stg_rental
stg_film
stg_inventory

Responsibilities:

- Rename columns
- Cast data types
- Remove unnecessary fields
- Standardize naming conventions

---

### 2️-Dimensional Model (Kimball)

A **star schema** is implemented for analytical queries.

Dimensions:
dim_customer
dim_film
dim_store
dim_staff
dim_date
dim_category
dim_actor

Fact tables:
fct_rental
fct_payment

Bridge tables (many-to-many relationships):
bridge_film_actor
bridge_film_category

Aggregations:
agg_payments_by_rental
agg_film_actors
agg_film_categories
customer_favorite_category

This model supports:

- Revenue analysis
- Customer behavior analysis
- Film performance analysis

---

### 3️-One Big Table (OBT)

The project also implements denormalized analytical tables.

These models simplify querying at the expense of redundancy.

Main OBT tables:
obt_rentals
obt_film_performance
obt_customer_value

These tables allow analysts to run queries without complex joins.

---

## Data Quality Tests

The project includes **built-in dbt tests** and **dbt-expectations** tests.

Examples:

- `not_null`
- `unique`
- `relationships`
- value range validation
- email format validation

Custom tests include:

- validating rental payments consistency
- ensuring no duplicate rental records in OBT tables

---

## dbt Packages Used
dbt-expectations
dbt-date

These packages provide advanced testing and date utilities.

---

## Running the Project
Install dependencies:
dbt deps

Run transformations:
dbt run

Execute tests:
dbt test
dbt docs generate
=======
Generate documentation:
dbt docs generate
dbt docs serve


---

## Example Analytical Questions
This model supports analyses such as:

- Monthly rental revenue
- Top performing films
- Customer lifetime value
- Store performance
- Customer favorite film categories

---

## Technologies Used
- PostgreSQL
- dbt
- Airbyte (data ingestion)
- DuckDB (development)
- GitHub

---

## Repository
GitHub repository:
https://github.com/cesargonza92/tp-final-dbt-dvdrental
