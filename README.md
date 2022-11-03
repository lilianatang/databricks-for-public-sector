# Lakehouse for the Public Sector
### Demo Scenario
The City of New York has access to massive volumes of geospatial data. By applying advanced analytics to these datasets, agencies can deliver on a broad range of use cases from transportation planning to disaster recovery and population health management. 
Therefore, the city would like to ingest and join millions of vehicle rides for spatial analysis on taxi pickup and dropoff patterns.
### Data Flow Architecture
![DataFlow](https://github.com/lilianatang/databricks-for-construction/blob/main/DataPipelineFlow.png?raw=true)

### Code Explanation
The main data processing code is stored in ```https://github.com/lilianatang/databricks-for-construction/blob/main/DLT%20Delta%20Pipeline%20Ingestion.py``` where we accomplished the following:
* Read point data and store raw data in 
* Read polygons data
* Process Unstructured Data (e.g. tokenization), train, and fine-tune a machine learning model to predict if an incident likely happenned at the job site based on the daily reports written by superintendents.
* Store raw employee data in ```employee_bronze(Age, Attrition, BusinessTravel, DailyRate, Department...etc)``` Delta table.
* Store raw predicted safety incidents in ```incident_bronze(text, location, prediction...etc)``` Delta table.
* Clean up and perform aggegration on raw employee data and store the processed data in ```employee_silver(location, headcount)``` Delta table
* Clean up and perform aggegration on raw predicted safety incidents data and store the processed data in ```incident_silver(location, number_of_predicted_incidents)``` Delta table
* Perform a RIGHT JOIN operation on ```employee_silver``` and ```incident_silver``` tables based on ```location``` to get golden records stored in ```employee_gold(location, headcount, number_of_predicted_incidents)``` for further descriptive analytics.

![HumanResourcesDashboard](https://github.com/lilianatang/databricks-for-construction/blob/main/HumanResourcesDashboard.png?raw=true)

### Why This Solution Saves Your Data Team So Much Time
In this demo, we accomplished a lot (reading two different datasets out of Azure Blob storage, processing both unstructured and structured data, trained and tuned a machine learning model on text data to predict if an incident likely happened at a job site, cleaning up data, performing aggregation on cleaned datasets, and creating golden records in ```employee_gold``` table) within only 100 lines of codes because we can leverage fully managed data science tools, such as MLFlow, optimized Apache Spark, and Delta Lake within Databricks. All your data team (data engineer, data analyst, and data scientist) can collaborate in one single unified platform without having to spin up a separate data lake (for Machine Learning works) in addition to a data warehouse (for building dashboards and reports).  