# no one
import pandas as pd
import numpy as np
from datetime import datetime
import random
import psycopg2
from io import StringIO  # Import StringIO
import csv




# Load the data
df = pd.read_csv(r"c:\Users\HP\Documents\RewardsData.csv")

# # a. Delete the tags column
df = df.drop('Tags', axis=1)
# print(df.columns)
# print(df.head())

# # # # b. Locate the empty cell on row 438, under the zip column and fill it with the number (11011)
if pd.isna(df.at[437, 'Zip']):  # row 438 is index 437
    df.at[437, 'Zip'] = '11011'

# # # c. In the zip column, truncate the numbers to the first 5 numbers
# df['Zip'] = df['Zip'].astype(str).str[:5]
# # print(pd.isna(df['Zip']).count())  # Check for NaN values in the Zip column

# # # d. In the zip column, populate all the empty cells with the mean value of the zip column
# # First convert to numeric, handling non-numeric values
# df['Zip'] = pd.to_numeric(df['Zip'], errors='coerce')
# # Calculate mean (ignoring NaN values)
# zip_mean = int(df['Zip'].mean())
# # Fill NaN values with mean
# df['Zip'] = df['Zip'].fillna(zip_mean).astype(int)

# # # e. In the city column, replace all instances of Winston Salem with the right capitalization
# df['City'] = df['City'].str.replace('Winston Salem', 'Winston-Salem')
# df['City'] = df['City'].str.replace('Winston-salem', 'Winston-Salem')
# df['City'] = df['City'].str.replace('Winston salem', 'Winston-Salem')
# df['City'] = df['City'].str.replace('winston salem', 'Winston-Salem')
# df['City'] = df['City'].str.replace('winston-salem', 'Winston-Salem')

# # # f. In the city column, remove every abbreviation and leave the cells empty
# # # Assuming abbreviations are single letters (like 'G' in row 6)
# df['City'] = df['City'].apply(lambda x: '' if isinstance(x, str) and len(x.strip()) == 1 else x)

# # # g. In the state column, replace every abbreviation with the full state names
# state_abbr = {
#     'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 
#     'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 
#     'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 
#     'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 
#     'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 
#     'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 
#     'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 
#     'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 
#     'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 
#     'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 
#     'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 
#     'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 
#     'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 
#     'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 
#     'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
# }

# df['State'] = df['State'].replace(state_abbr)

# # # h. Under the state column, replace all the empty cells with state names in alphabetical order
# states_ordered = sorted(state_abbr.values())
# empty_state_count = df['State'].isna().sum()
# state_cycle = states_ordered * (empty_state_count // len(states_ordered) + 1)
# df.loc[df['State'].isna(), 'State'] = state_cycle[:empty_state_count]

# # # i. Reformat the dates in the birthday column to the proper format
# def reformat_date(date_str):
#     if pd.isna(date_str):
#         return np.nan
#     try:
#         # Try parsing with different formats
#         for fmt in ('%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y'):
#             try:
#                 dt = datetime.strptime(str(date_str), fmt)
#                 return dt.strftime('%Y-%m-%d')
#             except ValueError:
#                 continue
#         return np.nan
#     except:
#         return np.nan

# df['Birthdate'] = df['Birthdate'].apply(reformat_date)

# # # j. Replace all the empty cells in the birthday column with random birth dates
# def random_date(start_year=1950, end_year=2005):
#     year = random.randint(start_year, end_year)
#     month = random.randint(1, 12)
#     day = random.randint(1, 28)  # Simple approach to avoid invalid dates
#     return f"{year}-{month:02d}-{day:02d}"

# df['Birthdate'] = df['Birthdate'].fillna(df['Birthdate'].apply(lambda x: random_date()))

# # # k. In the zip column, delete every row with numbers less than 5
# df = df[df['Zip'] >= 5]

# # # l. In the city column, populate all the empty cells with Thomasville
# df['City'] = df['City'].fillna('Thomasville')

# # Save the cleaned data
# df.to_csv('Cleaned_RewardsData.csv', index=False)

# print("Data cleaning complete. Saved to Cleaned_RewardsData.csv")

# df = pd.read_csv('Cleaned_RewardsData.csv')
# df = df.drop('Tags', axis=1)
# df.to_csv('RewardsData.csv', index=False)
# print("Data cleaning complete. Saved to Cleaned_RewardsData.csv")
# print(df.columns)
# print(df.info)

# df = pd.read_csv('Cleaned_RewardsData.csv')

# # Loading to data base


# def copy_from_csv(conn, table_name, csv_file_path, delimiter=',', null_string=''):
#     """
#     Copies data from a CSV file into a PostgreSQL table using the COPY command.
#     This is the most efficient way to load large amounts of data.

#     Args:
#         conn: psycopg2 connection object.
#         table_name (str): The name of the table to copy data into.
#         csv_file_path (str): Path to the CSV file.
#         delimiter (str, optional): The delimiter used in the CSV file. Defaults to ','.
#         null_string (str, optional): The string representing NULL values in the CSV. Defaults to ''.
#     """
#     cursor = conn.cursor()
#     try:
#         with open(csv_file_path, 'r') as f:
#             reader = csv.reader(f, delimiter=delimiter)
#             header = next(reader)  # Read and discard the header row
#             csv_file = StringIO()
#             writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
#             for row in reader:
#                 # Check if the row has more columns than the header
#                 if len(row) > len(header):
#                     row = row[:len(header)]  # Truncate the row to match the header
#                 elif len(row) < len(header):
#                     # Pad the row with empty strings if it has fewer columns
#                     row += [''] * (len(header) - len(row))
#                 writer.writerow(row)
#             csv_file.seek(0)

#             cursor.copy_from(csv_file, table_name, sep=delimiter, null=null_string)
#             conn.commit()
#             print(f"Data from '{csv_file_path}' successfully copied to table '{table_name}'.")

#     except psycopg2.Error as e:
#         print(f"Error copying data from CSV to table: {e}")
#         conn.rollback()  # Rollback the transaction on error
#         raise  # Re-raise the exception to be handled by the caller, if needed.
#     finally:
#         cursor.close()  # Ensure the cursor is closed.

# def create_table_from_csv(conn, table_name, csv_file_path,  null_string=''):
#     """
#     Creates a PostgreSQL table from a CSV file, inferring column names and data types
#     from the CSV file's header row.  Handles potential issues with empty fields and
#     attempts to handle different CSV dialects.

#     Args:
#         conn: psycopg2 connection object.
#         table_name (str): The name of the table to create.
#         csv_file_path (str): Path to the CSV file.
#         null_string (str, optional): The string representing NULL values in the CSV.
#     """
#     cursor = conn.cursor()
#     try:
#         with open(csv_file_path, 'r') as f:
#             # Use csv.Sniffer to detect the delimiter and quotechar
#             dialect = csv.Sniffer().sniff(f.read(1024))  # Read a chunk to sniff
#             f.seek(0)  # Reset file position after sniffing
#             reader = csv.reader(f, dialect)
#             header = next(reader)  # Get the header row

#             # Determine data types from the first row of data (after the header)
#             first_data_row = next(reader, None)  # Get the first data row or None if empty
#             if not first_data_row:
#                 raise ValueError("CSV file is empty or contains only a header.")
#              # Infer data types.  Handles empty strings correctly now.
#             column_types = []
#             for value in first_data_row:
#                 if value == null_string:  # Check for your NULL string
#                     column_types.append('TEXT')  # Default to TEXT for NULLs
#                 else:
#                     try:
#                         int(value)
#                         column_types.append('INTEGER')
#                     except ValueError:
#                         try:
#                             float(value)
#                             column_types.append('REAL')
#                         except ValueError:
#                             column_types.append('TEXT')  # Fallback to TEXT

#         # Construct the CREATE TABLE statement.
#         columns = [f"{header[i]} {column_types[i]}" for i in range(len(header))]
#         create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
#         print(f"Creating table: {create_table_query}")  # Print the query
#         cursor.execute(create_table_query)
#         conn.commit()
#         print(f"Table '{table_name}' successfully created.")

#     except psycopg2.Error as e:
#         print(f"Error creating table: {e}")
#         conn.rollback()
#         raise
#     except ValueError as e:
#         print(f"Error determining column types or CSV format: {e}")
#         conn.rollback()
#         raise
#     finally:
#         cursor.close()


# def main():
#     """
#     Main function to connect to the database, create a table, and copy data from a CSV file.
#     """
#     # 1.  Database connection details (replace with your actual details)
#     dbname = "rewards_data"  # Replace with your database name
#     user = "postgres"  # Default PostgreSQL user
#     password = "best1234"
#     host = "localhost"  # e.g., 'localhost' or an IP address
#     port = "5432"  # Default PostgreSQL port

#     # 2. CSV file path and table name
#     csv_file_path = "Cleaned_RewardsData.csv"  # Replace with your CSV file path
#     table_name = "reward_data"  # Replace with your desired table name

#     try:
#         # 3. Establish database connection
#         conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
#         conn.autocommit = False # Start a transaction

#         # 4. Create the table (if it doesn't exist)
#         create_table_from_csv(conn, table_name, csv_file_path)

#         # 5. Copy data from the CSV file to the table
#         copy_from_csv(conn, table_name, csv_file_path)

#         conn.commit() # Explicitly commit the transaction
#         print("Transaction completed successfully.")

#     except psycopg2.Error as e:
#         print(f"Database error: {e}")
#         # IMPORTANT:  No conn.rollback() here.  It's handled in the functions.
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         # No conn.rollback() here either.
#     finally:
#         if conn:
#             conn.close()
#             print("Connection closed.")

# if __name__  == "__main__":
#     main()
