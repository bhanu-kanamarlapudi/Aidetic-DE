import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime


# Function to create a MySQL connection
def create_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None


# Function to close the MySQL connection
def close_connection(connection):
    if connection:
        connection.close()
        print("Connection closed.")


# Function to read CSV and insert into MySQL
def csv_to_mysql(csv_file, table_name, host, user, password, database):
    # Read CSV into a Pandas DataFrame
    df = pd.read_csv(csv_file)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    # Convert 'Time' column to 'HH:MM:SS' format
    df['Time'] = pd.to_timedelta(df['Time'], errors='coerce').dt.total_seconds().fillna(0).astype(int).apply(lambda x: str(datetime.utcfromtimestamp(x))[11:19] if x != 0 else None)
    # Create a MySQL connection
    connection = create_connection(host, user, password, database)
    if connection is None:
        return

    try:
        # Create a cursor object using the connection
        cursor = connection.cursor()

        # Create a table in the database (if not exists)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
        """
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully")
        # Insert data into the MySQL table
        for _, row in df.iterrows():
            row = [None if pd.isna(value) else value for value in row]
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df.columns))])})"
            cursor.execute(insert_query, tuple(row))

        # Commit changes
        connection.commit()
        print(f"Data inserted into the '{table_name}' table successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        close_connection(connection)


# Replace these values with your own MySQL credentials and file paths
host = "localhost"
user = "root"
password = ""
database = "aidetic"
csv_file = "../database.csv"
table_name = "neic_earthquakes"

# Call the function to import CSV data into MySQL
csv_to_mysql(csv_file, table_name, host, user, password, database)
