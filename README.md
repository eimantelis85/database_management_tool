# Database Management Tool
This is a Python-based tool for managing PostgreSQL databases. It provides functionalities to create new databases, connect to existing databases, ingest data from CSV files into database tables, normalize data in database tables, and create new users for the PostgreSQL server.

## Prerequisites
Before running the tool, make sure you have the following installed:

Python installed on your system
PostgreSQL server

## Setup

1. Clone this repository to your local machine: https://github.com/eimantelis85/database_management_tool.git
2. Copy the `.env.example` file to `.env`.
3. Open the `.env` file in a text editor and replace the placeholder values with your actual PostgreSQL connection details.
4. To install virtual environment run ('python3 -m venv venv') and then ('source ./venv/bin/activate').
5. Install required dependencies by writing ('pip install -r requirements.txt').

## Usage
1. Run the `main.py` script using Python.
2. Follow the on-screen prompts to perform various database management tasks, such as creating a new database, connecting to an existing database, ingesting data from a CSV file, normalizing data in a table, creating a new user, or exiting the tool.

## Files

1. database_management.py: 
Contains the DatabaseManager class which provides functionalities to interact with a PostgreSQL database. Here's a summary of what it includes:

- Database Connection: The connect_to_database method establishes a connection to a PostgreSQL database using the provided credentials from environment variables.
- Database Creation: The create_database method allows creating a new database on the PostgreSQL server.
- Showing Connected Database: The show_connected_database_name method retrieves and prints the name of the currently connected database.
- CSV Data Ingestion: The ingest_csv_into_database method ingests data from a CSV file into a specified database table.
- Table Existence Check: The table_exists method checks if a specified table exists in the database.
- User Setup for PostgreSQL: The setup_user_for_postgresql method sets up a new user for managing a PostgreSQL database.
- Retrieving Table Columns: The get_table_columns method retrieves all columns of a specified table from the database.
- Table Normalization (2NF): The normalise_table_into_2NF method creates a new table from an existing table with selected columns and enforces second normal form (2NF) by removing redundant data.

2. main.py: This file contains the main script that interacts with the database using the `DatabaseManager` class from the `utils.database_management` module.

## Author
Eimantas Venslovas


