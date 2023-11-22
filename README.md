# Aidetic-DE
## Earthquake Analysis with PySpark
This project performs analysis on earthquake data using PySpark SQL and DataFrames.

## Data
The data is stored in a MySQL database table named neic_earthquakes with the following schema:
```sql
CREATE TABLE IF NOT EXISTS neic_earthquakes (
    `Date` DATE,
    `Time` TIME,
    `Latitude` DECIMAL(10, 6),
    `Longitude` DECIMAL(10, 6),
    `Type` VARCHAR(255),
    `Depth` DECIMAL(10, 2),
    `Depth Error` DECIMAL(10, 2),
    `Depth Seismic Stations` INT,
    `Magnitude` DECIMAL(3, 1),
    `Magnitude Type` VARCHAR(255),
    `Magnitude Error` DECIMAL(3, 1),
    `Magnitude Seismic Stations` INT,
    `Azimuthal Gap` DECIMAL(5, 2),
    `Horizontal Distance` DECIMAL(10, 2),
    `Horizontal Error` DECIMAL(10, 2),
    `Root Mean Square` DECIMAL(5, 2),
    `ID` VARCHAR(255),
    `Source` VARCHAR(255),
    `Location Source` VARCHAR(255),
    `Magnitude Source` VARCHAR(255),
    `Status` VARCHAR(255)
    );
```

## Requirements
To run this code, you need:

1. Python 3
2. PySpark 3.0+
3. pandas
4. MySQL Connector Python module
5. MySQL JAR Connector - https://dev.mysql.com/downloads/connector/j/<br/>

To install python modules, run the following command: ```pip install -r requirements.txt```
   
## Configuration
> Important:
> Update the following constants in the code to match your database configuration:


1. host = "localhost"
2. user = ""
3. password = ""
4. database = "aidetic" #dbname
5. csv_file = "../database.csv" #filelocation
6. table_name = "neic_earthquakes"

## Running the Code
To execute the PySpark script for this analysis:

1. Ensure you meet all the requirements and configuration steps above
2. Navigate to the project directory: /src/
3. To upload data from CSV to MySQL table, run the following command:
   ```python3 csv_to_mysql_upload.py```
4. Follow these instructions to read data and execute queries:
   1. To execute all queries, use the following command: ```spark-submit --jars ../mysql-connector-j-8.2.0/mysql-connector-j-8.2.0.jar spark_df_queries.py --all all```
   2. To execute a specific query, use the following command: ```spark-submit --jars ../mysql-connector-j-8.2.0/mysql-connector-j-8.2.0.jar spark_df_queries.py --questionNum 1```
   3. For Query-2, the default year is set to 2015. To execute for a different year, add the following argument to the above commands: ```--yearOfInterest 1995```
5. The output results will be displayed
