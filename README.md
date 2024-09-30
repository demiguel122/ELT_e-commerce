# Medallion Architecture for an e-commerce platform using Fivetran, Snowflake and dbt

## Table of contents

* [High-level Project Introduction](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#high-level-project-introduction)
* [Lakehouse Medallion Architecture](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#lakehouse-medallion-architecture)
  * [Bronze Layer](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#bronze-layer)
  * [Silver Layer](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#silver-layer)
  * [Gold Layer](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#gold-layer)
* [Other Project Aspects](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#other-project-aspects)
  * [Incrementality and Slowly Changing Dimensions (SCDs)](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#incrementality-and-slowly-changing-dimensions-scds)
  * [Testing](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#testing)
* [Contact](https://github.com/demiguel122/lakehouse_ELT_e-commerce?tab=readme-ov-file#contact)
-------------------

## High-level Project Introduction

This project implements a Medallion Architecture (ELT) using Modern Data Stack tools such as Fivetran, Snowflake and dbt. The ficticious organization is an e-commerce company.

Data is retrieved from the following sources:

- **Microsoft SQL Server**: _orders_, _order_items_, _products_, _promos_, _addresses_, _users_ and _events_ tables are extracted from. 
- **Google Sheets**: this is the origin of the table _budget_.
- **Meteostat**: this Python library provides a simple API for accessing open weather and climate data. This is where the table _weather_data_ was accessed from.

Data corresponding to Microsoft SQL Server and Google Sheets will be extracted and ingested with Fivetran. Data from Meteostat will be handled with a custom Python script that can also be found in this repository.

All tables except from _weather_data_ represent business-related entities. _weather_data_ contains historical weather data per zipcode and date. It is intended to serve advanced analytics purposes, such as adjusting a linear regression model to analyze the influence of weather upon sales.

As visible in the diagram below, I followed an ELT (Extract, Transform, Load) approach, whereby data is first extracted and directly loaded onto the Data Warehouse prior to its transformation with dbt. 

Additionally, and as part of a medallion architecture, data will be logically organized in three different stages: 
- **Bronze Layer**: it contains raw data. The table structures in this layer correspond to the source system table structures "as-is".
- **Silver Layer**: this is where the data from the Bronze layer is matched, merged, conformed and cleansed ("just enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions. From a data modeling perspective, the Silver Layer has more 3rd-Normal Form like data models.
- **Golden Layer**: Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models (i.e. Inmon, Kimball).

The goal of such design pattern is to incrementally and progressively improve the structure and quality of data as it flows through each layer of the architecture.

<p align="center">
  <img src="https://github.com/demiguel122/ELT_Snowflake_dbt_e-commerce/assets/144360549/64eeb717-2349-44e6-91de-8ad2d68dbbc4.png">
</p>

## Lakehouse Medallion Architecture

### Bronze Layer

Below we can find the Entity-Relationship Diagram (ERD) corresponding to the Bronze Layer. As we saw earlier, this is the data model as-is.

As such, all these tables were configured as source models in the dbt project.

<p align="center">
  <img src="https://github.com/demiguel122/ELT_Snowflake_dbt_e-commerce/assets/144360549/1a3f9621-4613-4171-9883-0168dea25dd0.png">
</p>

### Silver Layer

As per [dbt's official documentation](https://docs.getdbt.com/best-practices/how-we-structure/2-staging), staging models should have a 1-to-1 relationship to our source tables. That means for each source system table we’ll have a single staging model referencing it, acting as its entry point —staging it— for use downstream.

These models incorporate minor or light transformations (i.e. renaming, casting, basic computations, categorizations, hashing surrogate keys, etc). Hence, the ERD did not change at this stage of our project.

All the staging models of the project can be found [here](https://github.com/demiguel122/lakehouse_ELT_e-commerce/tree/main/models/staging).

### Gold Layer

This is the layer where everything comes together and we start to arrange all of our staging models into full-fledged cells that have identity and purpose. In dbt, this layer is commonly refered to as the marts layer. 

Grouping models by departments (marketing, finance, etc) is the most common structure at this stage. In this project, marts are grouped into four different folders: _core_, _marketing_, _product_ and _advanced_analytics_, each of which containing different joined, project-specific models for each department.

The **_core_** folder contains all models that are of common use to all different departments and busines units of the organization, and they will be later used to create department-specific models. All the models in this folder conform our Kimball-like dimensional model. Following Kimball's guidelines, the following changes were made to the original E-R model:

- **Removed the _orders_ table, kept _order_items_ only**: The initial model had two tables that represented sales transactions: _orders_, which contained data at the header level, and _order_items_, with data at line level. We lowered the grain to the line level. 'order_cost_item_usd' was taken directly from the 'price_usd' field in the _products_ table, so there was no need for any additional computation. In order to get the unitary shipping cost, 'shipping_cost_item_usd', we had to re-calculate through a process called _allocation_, whereby the shipping cost is proportionally allocated to each order item in accordance with their relative weight in the overall order cost:
```
(price_usd / order_total_usd) * shipping_cost_usd AS shipping_cost_item_usd
```
- **Added dimension tables**: some dimensions were taken out of the fact tables and included in their own dimension table (i.e. _dim_shipping_, _dim_status_ and _dim_event_type_). _dim_date_ and _dim_time_ were also generated.

The resulting dimensional model can be found below, with green tables being dimension tables and orange tables being fact tables (note that _dim_date_ and _dim_time_ were not included for the sake of simplicity):

<p align="center">
  <img src="https://github.com/demiguel122/lakehouse_ELT_e-commerce/assets/144360549/5f18bf14-a05e-476b-9832-4e4dd047878c.png">
</p>

All the models of the project corresponding to the Gold Layer can be found [here](https://github.com/demiguel122/lakehouse_ELT_e-commerce/tree/main/models/marts).

## Other Project Aspects

### Incrementality and Slowly Changing Dimensions (SCDs)

All the staging and numerous downstream models were configured as [incremental](https://docs.getdbt.com/docs/build/incremental-models) in dbt (by setting the _'materialized'_ config parameter to _'incremental'_):

```
{{
    config(
        materialized='incremental',
        unique_key=<unique key>
    )
}}
```

This reduces computation overhead (and its associated cost) every time the models are run by processing only the new/updated (i.e. the "delta") rows in each table.

The _'unique_key'_ parameter enables _updating_ existing rows instead of just appending new rows. If new information arrives for an existing unique key, that new information can replace the current information instead of being appended to the table. If a duplicate row arrives, it can be ignored. Note that this parameter is a form of implementing **Type-1 Slowly Changing Dimensions** in dbt, and was therefore convenient for those models in which we are only interested in keeping the latest version without any historical data, such as _fct_events_, _fct_budget_, _dim_status_, _dim_addresses_, and other.

On the other hand, dbt's [snapshots](https://docs.getdbt.com/docs/build/snapshots) were also used for models for which we needed historical data to "look back in time" at previous data states in their mutable tables. Snapshots implement **Type-2 Slowly Changing Dimensions**.

In such cases, I decided to use snapshots as upstream models for downstream incremental models. This way, the working model will only contain the latest version of data, but a historical version will always be available if needed. This is known as **Type-4 Slowly Changing Dimensions**. The diagram below shows an example:

<p align="center">
  <img src="https://github.com/demiguel122/lakehouse_ELT_e-commerce/assets/144360549/a83479ee-4d45-4126-be19-a606a72e4670.png">
</p>

### Testing

All models in each layer are consistently tested. These tests can be found in each .yml file. 

There are 2 different types of tests in dbt:

- **Singular tests**: these tests are defined in .sql files, typically in the tests directory. They are defined by writing the exact SQL that will return failing records. We call these "singular" data tests, because they're one-off assertions usable for a single purpose.

- **Generic tests**: these tests can be reused over and over again. When a test is generic, it can be defined on as many columns as you like, across as many models as you like.

The most common generic tests are _not_null_, _unique_ and _relationships_. These tests are already defined by dbt. In this project, I defined a _positive_values_ test.

Singular, specific tests were also defined. Here's an example:

```
SELECT
    order_cost_usd,
    shipping_cost_usd,
    order_total_usd,
    discount_usd
FROM {{ ref('stg_sql_server_dbo__orders') }}
JOIN {{ ref('stg_sql_server_dbo__promos') }}
USING(promo_key)
WHERE (order_cost_usd + shipping_cost_usd) - discount_usd != order_total_usd
```

This _stg_orders__order_total_ test checks whether the total order amount equals the order cost plus the shipping costs, once a potential discount is applied.

More singular tests can be found in the [tests directory](https://github.com/demiguel122/lakehouse_ELT_e-commerce/tree/main/tests).

## Contact

- **Linkedin**: [https://www.linkedin.com/in/albertodemiguel/](https://www.linkedin.com/in/albertodemiguel/)
- **Email**: [ademiguellechuga@gmail.com](ademiguellechuga@gmail.com)
