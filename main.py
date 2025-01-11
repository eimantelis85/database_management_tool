from utils.database_management import DatabaseManager
from utils.database_management import NormalizationManager
import os

def main():
    # Create an instance of the DatabaseManager
    db_manager = DatabaseManager()

    # Connect to the default database
    connection = db_manager.connect_to_database(os.getenv("PG_DATABASE"))
    db_manager.show_connected_database_name()
    if connection is None:
        print("Failed to connect to the default database.")
        return

    while True:
        print("\nSelect an option:")
        print("1. Create a new database")
        print("2. Connect to an existing database")
        print("3. Ingest data from CSV file to database")
        print("4. Normalize data in a database table")
        print("5. Remove repetitive rows from a database table")
        print("6. Create a new user for the database")
        print("7. Exit")
        option = input("Enter your choice (1/2/3/4/5/6/7): ")

        if option == "1":
            database_name = db_manager.create_database()
            if database_name:
                connection = db_manager.connect_to_database(database_name)
                db_manager.show_connected_database_name()
        elif option == "2":
            database_name = input("Enter the name of the database to connect to: ")
            connection = db_manager.connect_to_database(database_name)
            db_manager.show_connected_database_name()
        elif option == "3":
            table_name = input("Enter the name of the table: ")
            if db_manager.table_exists(table_name):
                print("Table already exists. Please choose a different name.")
                continue

            filename = input("Enter the name of the CSV file (without extension): ")
            file_path = f"data/{filename}.csv"
            if not os.path.exists(file_path):
                print("File does not exist.")
                continue
            
            if db_manager.ingest_csv_into_database(table_name, file_path):
                print(f"Table '{table_name}' created successfully.")
            else:
                print("Failed to create table from CSV file.")
        elif option == "4":
            table_to_normalise = input("Enter the name of the table you want to normalize: ")
            if db_manager.table_exists(table_to_normalise):
                new_table_name = input("Enter the name for the new table: ")
                if db_manager.table_exists(new_table_name):
                    print("Table name already exists. Please choose a different name.")
                    continue

                table_columns = db_manager.get_table_columns(table_to_normalise)
                print(f'Which of the columns should be placed in another table? ({", ".join(table_columns)})')

                selected_columns = input("Enter the column names separated by commas: ").strip().split(',')
                selected_columns = [col.strip() for col in selected_columns]
                
                if set(selected_columns).issubset(table_columns):
                    primary_key_name = input("Enter the name of the primary key column: ")
                    NormalizationManager.normalise_table_into_2NF(connection, table_to_normalise, new_table_name, selected_columns, primary_key_name)
                else:
                    print("Not all selected columns are in the original table. Please provide valid column names.")
            else:
                print("Table does not exist.")
        elif option == "5":
            table_name = input("Enter the name of the table to remove repetitive rows from: ")
            if db_manager.table_exists(table_name):
                new_table_name = input("Enter the name for the new table with distinct rows: ")
                if db_manager.table_exists(new_table_name):
                    print("Table name already exists. Please choose a different name.")
                    continue

                primary_key_name = input("Enter the name of the primary key column: ")
                if NormalizationManager.remove_repetitive_rows(connection, table_name, new_table_name, primary_key_name):
                    print(f"Repetitive rows removed from table '{table_name}' and new table '{new_table_name}' created successfully.")
                else:
                    print("Failed to remove repetitive rows from table.")
            else:
                print("Table does not exist.")
        elif option == "6":
            if db_manager.setup_user_for_postgresql():
                print("User created successfully.")
            else:
                print("Failed to create user.")
        elif option == "7":
            print("Exiting...")
            break
        else:
            print("Invalid option. Please select a valid option.")

    if connection is not None:
        connection.close()
        print("Connection to the database was closed.")

if __name__ == "__main__":
    main()