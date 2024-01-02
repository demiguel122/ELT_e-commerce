# Lakehouse for an e-commerce platform using Fivetran, Snowflake and dbt

## Table of contents

* [High-level Project Introduction](https://github.com/demiguel122/ELT_Snowflake_dbt_e-commerce/edit/main/README.md#high-level-project-introduction)
* [Medallion Lakehouse Architecture](https://github.com/demiguel122/ELT_Snowflake_dbt_e-commerce/edit/main/README.md#medallion-lakehouse-architecture)
  * [Bronze Layer](https://github.com/demiguel122/ELT_Snowflake_dbt_e-commerce/edit/main/README.md#bronze-layer)
  * [Silver Layer]()
  * [Gold Layer]()
* [Other Project Aspects]()
  * [Testing]()
  * [Incrementality and Slowly Changing Dimensions (SCDs)]()
-------------------

## High-level Project Introduction

This project implements a lakehouse medallion architecture using modern Data Stack tools such as Fivetran, Snowflake and dbt. The ficticious organization is an e-commerce company.

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



## Medallion Lakehouse Architecture



### Bronze Layer



<p align="center">
  <img src="https://github.com/demiguel122/ELT_Snowflake_dbt_e-commerce/assets/144360549/1a3f9621-4613-4171-9883-0168dea25dd0.png">
</p>
