import os
from dotenv import load_dotenv
import psycopg2
import pandas

class DatabaseManager:
    def __init__(self):
        load_dotenv()

        self.pg_host = os.getenv("PG_HOST")
        self.pg_user = os.getenv("PG_USER")
        self.pg_password = os.getenv("PG_PASSWORD")
        self.pg_port = os.getenv("PG_PORT")
        self.pg_database = os.getenv("PG_DATABASE")
        self.connection = None

    def connect_to_database(self, database_name):
        """
        Connects to a PostgreSQL database.

        Args:
            database_name (str): Name of the database to connect to. Defaults to an empty string.

        Returns:
            psycopg2.extensions.connection: A connection to the PostgreSQL database.
            None: If connection to the database fails.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.pg_host,
                user=self.pg_user,
                password=self.pg_password,
                port=self.pg_port,
                database=database_name,
            )
            self.connection.autocommit = True
            return self.connection
        except psycopg2.OperationalError as err:
            print("Failed to connect to PostgreSQL:", err)
            return None

    def create_database(self):
        """
        Creates a new database in the PostgreSQL server.

        Returns:
            str: The name of the newly created database.
            None: If database creation fails.
        """
        cursor = self.connection.cursor()
        database_created = False
        while not database_created:
            database_name = input("Please enter the name of your database to create: ")
            try:
                cursor.execute(f'CREATE DATABASE {database_name}')
                print("Database created successfully!")
                database_created = True
                return database_name
            except psycopg2.ProgrammingError as err:
                print("Failed to create database:", err)
                print("Please make sure the database name is valid and doesn't already exist.")
            except Exception as e:
                print("An unexpected error occurred:", e)
                break
        self.connection.autocommit = False
        cursor.close()
        return None

    def show_connected_database_name(self):
        """
        Retrieves the name of the currently connected database and prints it to the console.

        Returns:
            None
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT current_database();")
        current_db = cursor.fetchone()[0]
        print("Connected to database:", current_db)
        cursor.close()
        
    def ingest_csv_into_database(self, table_name, csv_file_path):
        """
        Ingests data from a CSV file into a database table.

        Args:
            table_name (str): The name of the table to insert data into.
            csv_file_path (str): The path to the CSV file.

        Returns:
            bool: True if ingestion is successful, False otherwise.
        """
        try:
            df = pandas.read_csv(csv_file_path)

            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
                table_exists = cursor.fetchone()[0]
                if not table_exists:
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                    for col, dtype in zip(df.columns, df.dtypes):
                        col = col.strip()
                        sql_type = 'TEXT' if dtype == 'object' else 'INTEGER' if dtype == 'int64' else 'FLOAT' if dtype == 'float64' else 'TEXT'
                        create_table_query += f'"{col}" {sql_type}, '
                    create_table_query = create_table_query.rstrip(", ") + ")"
                    
                    cursor.execute(create_table_query)
                    self.connection.commit()
            
            with self.connection.cursor() as cursor:
                columns = ', '.join(f'"{col.strip()}"' for col in df.columns)
                placeholders = ', '.join(['%s'] * len(df.columns))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                for index, row in df.iterrows():
                    row = [str(cell).strip() for cell in row]
                    cursor.execute(query, tuple(row))
                
                self.connection.commit()
            
            print(f"Data from '{csv_file_path}' ingested into table '{table_name}' successfully.")
            return True
        except Exception as e:
            print(f"An error occurred while ingesting data from CSV file into table: {e}")
            return False
        
    
    def table_exists(self, table_name):
        """
        Checks if a table exists in the database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
                table_exists = cursor.fetchone()[0]
                return table_exists
        except Exception as e:
            print(f"An error occurred while checking if table '{table_name}' exists: {e}")
            return False
        
    def setup_user_for_postgresql(self):
        """
        Sets up a new user for PostgreSQL.

        Returns:
            bool: True if user setup is successful, False otherwise.
        """
        try:
            username = input("Enter the new PostgreSQL username: ")
            password = input("Enter the password for the new user: ")
            database_name = input("Enter the name of the database for the new user to manage: ")

            with self.connection.cursor() as cursor:
                create_query = f"CREATE USER {username} WITH ENCRYPTED PASSWORD '{password}'; "
                create_query += f"GRANT CONNECT ON DATABASE {database_name} TO {username}; "
                create_query += f"GRANT ALL PRIVILEGES ON DATABASE {database_name} TO {username}"
                cursor.execute(create_query)
                self.connection.commit()

            print(f"User '{username}' created successfully for database '{database_name}'.")
            # loging
            return True
        except Exception as e:
            print(f"An error occurred during user setup: {e}")
            return False
        
    def get_table_columns(self, table_name):
        """
        Retrieves all columns of a specified table from the database.

        Args:
            table_name (str): The name of the table.

        Returns:
            list: A list of column names.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                columns = [row[0] for row in cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"An error occurred while retrieving columns for table '{table_name}': {e}")
            return []
        
class NormalizationManager:
    @staticmethod

    def remove_repetitive_rows(connection, table_name, new_table_name, primary_key_name):
        """
        Create a new table with distinct rows from the selected table and generate a primary key for each row.

        Args:
            connection (psycopg2.extensions.connection): Connection to the PostgreSQL database.
            table_name (str): Name of the existing table.
            new_table_name (str): Name for the new table to store distinct rows.
            primary_key_name (str): Name for the primary key column.

        Returns:
            bool: True if new table with distinct rows was created successfully, False otherwise.
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE {new_table_name} AS
                    SELECT ROW_NUMBER() OVER () AS {primary_key_name}, t.* FROM (
                        SELECT DISTINCT * FROM {table_name}
                    ) t
                """)

                connection.commit()
                
                print(f"New table '{new_table_name}' created with distinct rows and generated primary keys successfully.")
                return True
        except Exception as e:
            print(f"An error occurred while creating new table with distinct rows and generated primary keys: {e}")
            return False

    def normalise_table_into_2NF(connection, table_name, new_table_name, table_columns, primary_key_name):
        """
        Creates a new table from an existing table with selected columns. 
        Connects tables with primary and foreign keys. 
        Deletes selected columns from an existing table.

        Args:
            connection (psycopg2.extensions.connection): Connection to the PostgreSQL database.
            table_name (str): Name of the existing table.
            new_table_name (str): Name of the new table to be created.
            table_columns (list of str): List of column names to be included in the new table.

        Returns:
            bool: True if the new table is created successfully, False otherwise.
        """
        try:
            cursor = connection.cursor()
            create_query = f"CREATE TABLE {new_table_name} AS SELECT DISTINCT "
            for column in table_columns:
                create_query += f'"{column}", '
            create_query = create_query[:-2]
            create_query += f" FROM {table_name}; "
            create_query += f'ALTER TABLE {new_table_name} ADD COLUMN "{primary_key_name}" SERIAL PRIMARY KEY; '
            create_query += f'ALTER TABLE {table_name} ADD COLUMN "{primary_key_name}" INTEGER; UPDATE {table_name} SET "{primary_key_name}" = {new_table_name}."{primary_key_name}" FROM {new_table_name} WHERE {' AND '.join([f'{table_name}."{column}" = {new_table_name}."{column}"' for column in table_columns])}; ALTER TABLE {table_name} ADD CONSTRAINT fk_{primary_key_name} FOREIGN KEY ("{primary_key_name}") REFERENCES {new_table_name}("{primary_key_name}"); '
            create_query += f'ALTER TABLE {table_name} '
            create_query += ",\n".join([f'DROP COLUMN "{column}"' for column in table_columns])
            cursor.execute(create_query)
            connection.commit()
            cursor.close()
            print(f"Table '{new_table_name}' created with selected columns from '{table_name}'.")
            print(f"A primary key '{primary_key_name}' was added to '{new_table_name}'.")
            print(f"A foreign key '{primary_key_name}' was added to '{table_name}' to reference '{new_table_name}'.")
            print(f"Selected columns and repetitive data were removed from '{table_name}'.")
            return True
        except Exception as e:
            print(f"Failed to create table: {e}")
            return False