# Azure Data Engineering Project - Car Sales Analysis

## 🚀 Project Overview
This project is an **end-to-end Azure Data Engineering solution** for analyzing car sales data. It follows the **Medallion Architecture (Bronze, Silver, Gold layers)** to process raw data, transform it, and create analytical insights. The project involves **Azure Data Factory, Azure Databricks, and Azure Data Lake** for efficient ETL and data processing.

![Image](https://github.com/user-attachments/assets/d6d25121-95a2-499e-bf7f-f50e966beba8)

## 📌 Technologies & Tools I have Used
- **Azure Data Factory (ADF)** - Data ingestion & orchestration
- **Azure Databricks** - Data transformation using PySpark
- **Azure Data Lake Storage Gen2** - Data storage in Parquet & Delta format
- **Azure Event Hub** - Real-time data processing
- **Azure SQL Database** - Source system for transactional data
- **Power BI** - Data visualization & reporting
- **GitHub** - Version control & project management

## 🛠 Architecture & Implementation
### 1️⃣ **Data Ingestion (Bronze Layer)**
- Extract data from **SQL Database** using **Azure Data Factory**
- Implement **incremental loading** using **watermark strategy**
- Store raw data in **Azure Data Lake (Parquet format)**

  ![Image](https://github.com/user-attachments/assets/26a86107-3d60-4166-bae7-455d2eb9e9d1)
  
### 2️⃣ **Data Transformation (Silver Layer)**
- Process raw data using **Databricks & PySpark**
- Clean, deduplicate, and join tables for a structured format
- Store transformed data in **Azure Data Lake (Parquet format)**

### 3️⃣ **Data Aggregation & Serving (Gold Layer)**
- Implement **Dimensional Modeling (Star Schema)** with:
  - **Fact_sales** table for aggregated sales transactions
  - **Dimension tables:** Dealers, Branches, Dates, and Models
- Convert data into **Delta Lake format** for faster queries & ACID compliance
- Load into **Power BI** for reporting

  ![Image](https://github.com/user-attachments/assets/97ddc9c1-1707-4530-9e2c-87b5e5de54ee)
  
## 📚 What I Learned
- **End-to-End Data Pipeline Development** using Azure services
- **Efficient ETL Processing** with **ADF & Databricks Workflows**
- **Data Lakehouse Architecture** (Medallion - Bronze, Silver, Gold)
- **Incremental Data Loading** using **watermarking & Delta Lake CDC**
- **Optimizing Data Queries** using **Delta Lake & Partitioning Strategies**
- **Building Analytical Dashboards** using Power BI
- **Version Control & Collaboration** with GitHub

## 🎯 Conclusion
In This Project I have Gained **hands-on experience in designing scalable data pipelines** using Azure. By leveraging **modern cloud-based data engineering techniques**, I successfully **ingested, transformed, and analyzed sales data**. The integration of **Databricks, Delta Lake, and Power BI** enabled efficient real-time insights and **optimized performance for large datasets**.
