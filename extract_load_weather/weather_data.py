from datetime import datetime
import pgeocode
from meteostat import Point, Daily
import pandas as pd
from dotenv import load_dotenv
import os
import snowflake.connector

# List of zip codes for locations
zip_codes = [
    92056, 85062, 21282, 62723, 33169, 24029, 19172, 35220, 75044, 90805, 24009,
    80005, 22093, 14263, 52804, 14604, 6912, 94705, 44310, 27455, 90015, 79940,
    59105, 20551, 89087, 85271, 94105, 33261, 60505, 31217, 20508, 28230, 93094,
    35254, 87592, 40210, 12242, 10060, 31605, 23605, 98042, 77240, 94975, 22405,
    48206, 47732, 45296, 33884, 76796, 54915, 75185, 15266, 77070, 32215, 97216,
    29424, 30919, 22217, 47937, 23464, 20046, 35263, 85205, 75231, 49018, 46221,
    55407, 91328, 73124, 97221, 20525, 2208, 28235, 93291, 25716, 98417, 79165,
    22119, 80279, 2203, 92160, 80940, 94660, 89505, 79923, 67205, 18105, 73104,
    92825, 32868, 47747, 3804, 79491, 64144, 20078, 85083, 78426, 36177, 78220,
    39236, 19805, 78769, 76505, 11388, 2458, 66642, 55172, 76205, 32405, 65805,
    50320, 88530, 75074, 20337, 89130, 44511, 98175, 93762, 21211, 95150, 64142,
    92822, 11236, 21229, 10469, 76011, 2305, 65218, 80150, 18763, 76705, 45454,
    55417, 29505, 32259, 92844, 55446, 68144, 34985, 77005, 71137, 78405, 35810,
    66617, 20456, 76192
]

# Use pgeocode to get latitude and longitude for each zip code
location_data = {}
nomi = pgeocode.Nominatim('US')

for zip_code in zip_codes:
    location_info = nomi.query_postal_code(zip_code)
    if not location_info.empty:
        latitude, longitude = location_info.loc[['latitude', 'longitude']]
        location_data[zip_code] = {'latitude': latitude, 'longitude': longitude}

# Set time period
start = datetime(2021, 2, 10)
end = datetime(2021, 3, 5)

# Initialize an empty DataFrame to store the results
weather_data = pd.DataFrame()

# Fetch weather data for each location using Meteostat
for zip_code, coordinates in location_data.items():
    point = Point(coordinates['latitude'], coordinates['longitude'])
    daily_data = Daily(point, start, end).fetch()
    daily_data = daily_data[['tavg', 'tmin', 'tmax', 'prcp']]

    # Add a "zipcode" column with the corresponding zip code
    daily_data['zipcode'] = str(zip_code)

    # Add a "time" column with the date, convert it to datetime and extract the date part only
    daily_data['date'] = daily_data.index
    daily_data['date'] = pd.to_datetime(daily_data['date']).dt.date

    # Add a "row_id" column as a unique identifier
    daily_data['row_id'] = daily_data['zipcode'].astype(str) + '_' + daily_data['date'].astype(str)

    # Add an empty "date_loaded" column
    daily_data['date_loaded'] = pd.NaT

    # Reorder the columns
    daily_data = daily_data[['row_id', 'zipcode', 'date', 'tavg', 'tmin', 'tmax', 'prcp', 'date_loaded']]

    # Rename columns
    daily_data = daily_data.rename(columns={'tavg': 'avg_temperature_celsius',
                                            'tmin': 'min_temperature_celsius',
                                            'tmax': 'max_temperature_celsius',
                                            'prcp': 'precipitation'})

    # Cast remaining columns to specific data types
    daily_data['zipcode'] = daily_data['zipcode'].astype(str)
    daily_data['avg_temperature_celsius'] = daily_data['avg_temperature_celsius'].astype(float)
    daily_data['min_temperature_celsius'] = daily_data['min_temperature_celsius'].astype(float)
    daily_data['max_temperature_celsius'] = daily_data['max_temperature_celsius'].astype(float)
    daily_data['precipitation'] = daily_data['precipitation'].astype(float)
    daily_data['date_loaded'] = daily_data['date_loaded'].astype('datetime64[ns]')

    # Replace NULL values with 0 in all float columns
    float_columns = daily_data.select_dtypes(include=['float']).columns
    daily_data[float_columns] = daily_data[float_columns].fillna(0)

    # Concatenate data to the overall DataFrame
    weather_data = pd.concat([weather_data, daily_data.reset_index(drop=True)], ignore_index=True)

# Connecting to Snowflake, creating a temporary .csv file in local and loading it to an internal Snowflake stage
load_dotenv()

snowflake_config = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'stage_name': os.getenv('SNOWFLAKE_STAGE')
}

conn = snowflake.connector.connect(
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    account=snowflake_config['account'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema=snowflake_config['schema']
)

cursor = conn.cursor()

try:
    # Get the path to the desktop for the current user
    desktop_path = os.path.expanduser("~/Desktop")
    temp_csv_path = os.path.join(desktop_path, "weather_data.csv")

    # Create the temporary .csv file
    weather_data.to_csv(temp_csv_path, index=False, header=False)

    # Upload data to Snowflake stage
    cursor.execute(f"PUT file://{temp_csv_path} @{snowflake_config['stage_name']} OVERWRITE = TRUE ")

    # Remove the temporary .csv file
    os.remove(temp_csv_path)

    print("Weather data successfully uploaded to Snowflake stage.")

except snowflake.connector.errors.Error as e:
    print(f"Error uploading weather data to Snowflake stage: {e}")

# Copy the data from the stage into the target table
try:
    table_name = 'weather_data'
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ( "
                   f"row_id INT, "
                   f"zipcode VARCHAR(5), "
                   f"date DATE, "
                   f"avg_temperature_celsius NUMBER, "
                   f"min_temperature_celsius NUMBER, "
                   f"max_temperature_celsius NUMBER, "
                   f"precipitation NUMBER, "
                   f"date_loaded TIMESTAMP) "
    )

    # Create a temporary table in Snowflake with the same structure as the target table
    cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {table_name}_temp AS "
                   f"SELECT * FROM {table_name} WHERE 1=0")

    # Copy data from the Snowflake stage to the temporary table
    cursor.execute(f"COPY INTO {table_name}_temp "
                   f"FROM @{snowflake_config['stage_name']}/ "
                   f"FILES = ('weather_data.csv.gz') "
                   f"FILE_FORMAT = (TYPE = CSV) "
                   )

    # Merge data from the temporary table into the main table (update existing rows, insert new rows)
    cursor.execute(f"MERGE INTO {table_name} AS target "
                   f"USING {table_name}_temp AS source "
                   f"ON target.row_id = source.row_id "
                   f"WHEN MATCHED THEN UPDATE SET "
                   f"target.zipcode = source.zipcode, "
                   f"target.date = source.date, "
                   f"target.avg_temperature_celsius = source.avg_temperature_celsius, "
                   f"target.min_temperature_celsius = source.min_temperature_celsius, "
                   f"target.max_temperature_celsius = source.max_temperature_celsius, "
                   f"target.precipitation = source.precipitation "
                   f"WHEN NOT MATCHED THEN INSERT (row_id, zipcode, date, "
                   f"avg_temperature_celsius, min_temperature_celsius, max_temperature_celsius, precipitation, date_loaded) "
                   f"VALUES (source.row_id, source.zipcode, source.date, "
                   f"source.avg_temperature_celsius, source.min_temperature_celsius, "
                   f"source.max_temperature_celsius, source.precipitation, current_timestamp()) "
                   )

    # Remove the temporary table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}_temp")

    print("Weather data successfully copied from stage to Snowflake table.")

except snowflake.connector.errors.Error as e:
    print(f"Error copying weather data from stage to Snowflake table: {e}")

# Close the Snowflake connection
conn.close()