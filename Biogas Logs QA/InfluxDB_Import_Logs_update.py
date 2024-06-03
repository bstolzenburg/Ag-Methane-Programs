# Importing Modules
import pandas as pd
from pathlib import Path
import yaml
import os
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# Defining global variables for InfluxDB Client Information
token = "HTJGo6WzgHxIZn6Fv-MdfUG-nq2gQyKdqgexYAxpX4cqGvAusZYhevTbNc3jCBCpxZoRjKN6tYDpNGtcWvdpVQ=="  # Update with the new token
org = "Ag Methane"
url = 'http://localhost:8086/'

# Main Function
def main():
    # Setting directory to Github directory in the 'software' folder
    os.chdir(SetDirectory('software'))

    # Getting file locations from Gaslog_file_locations YAML file
    file_paths = GetFileLocations()

    # Setting directory to the 'shortcuts' folder
    os.chdir(SetDirectory('shortcut'))

    # Running influxd.exe daemon to start server session
    os.startfile('Run InfluxDB.lnk')

    # Setting directory to 'gaslogs' folder
    os.chdir(SetDirectory('gaslogs'))

    # Reading file_paths dictionary and calling the ProcessLogs() function to process the raw .csv logs and prepare for input to Influx server
    for farm, path in file_paths.items():
        # Calling ProcessLogs() function to return dictionary containing processed logs dataframes for each farm
        logs_dict = ProcessLogs(farm, path)

        # Uploading processed logs into InfluxDB
        print('Uploading logs for ', farm)
        print('')

        # Iterating through logs_dict dictionary and pulling out dataframes to upload into InfluxDB
        for device, df in logs_dict.items():
            print('Uploading ', device, ' logs')
            print('')
            success = InfluxInput(df, device, farm)
            if success:
                print(f'Successfully uploaded {device} logs for {farm}')
            else:
                print(f'Failed to upload {device} logs for {farm}')
            print('')
    print('Finished uploading data')

# Function to return unique file paths from home directory
def SetDirectory(str):
    # Getting home path
    home = Path.home()
    
    # Getting path to directories
    if str == 'gaslogs':
        # Getting path for gaslogs directory
        gaslog_dir = os.path.join(home, 'Patrick J Wood Dropbox', '_operations', 'gaslogs')
        return gaslog_dir

    if str == 'software':
        # Getting path for software directory
        software_dir = os.path.join(home, 'Patrick J Wood Dropbox', '_operations', 'Software', 'Github', 'Ag-Methane-Programs', 'Biogas Logs QA')
        return software_dir

    if str == 'shortcut':
        # Getting path to shortcuts directory
        shortcut_dir = os.path.join(home, 'Patrick J Wood Dropbox', '_operations', 'Software', 'Shortcuts')
        return shortcut_dir

# Function to read Gaslog_file_locations.yml and compile file locations into dictionary, returns dictionary with farm name as the key, file path as the value
def GetFileLocations():
    # Setting file location for YAML config file
    config = r'Gaslog_file_locations_24.yml'

    # Reading the YAML file and getting file paths dictionary
    with open(config) as file:
        dict = yaml.load(file, Loader=yaml.FullLoader)

    # Creating blank dictionary to store file paths
    file_paths = {}

    # Create filepaths for each farm
    for farm, value in dict.items():
        # Create file path from value
        file_path = os.path.join(value[0], value[1])
        # Append farm name and file path to dictionary
        file_paths.update({farm: file_path})

    # Return the dictionary
    return file_paths

# Function to read dictionary containing file paths for each farm, and to read the logs into separate pandas df by device
# Returns dictionary containing dataframe for each device
def ProcessLogs(farm, path):
    print('Downloading logs for ', farm)
    print('')

    # Creating empty dictionary to store dataframes
    logs = {}

    # Reading in the raw log .csv at the designated file path (taken from the dictionary)
    df_raw = pd.read_csv(path, encoding='ISO-8859-1', parse_dates={'Timestamp': ['Date', 'Time']}, index_col=False)

    # Adding a tag column that is equal to the farm name
    df_raw['Tag'] = farm

    # Fixing Hanford kWh output columns that have non-descriptive names
    if farm == 'Hanford':
        df_raw.rename(columns={'KWH Output': 'Engine 1 KWH Output', 'KWH Output.1': 'Engine 2 KWH Output',
                               'KWH Output.2': 'Engine 3 KWH Output'}, inplace=True)

    # Subsetting raw dataframe for columns related to each device
    # Engine keyword searches
    engine_search = ['Timestamp', 'Tag', 'Engine', 'engine', 'G1', 'G2', 'G3', 'Gen']

    # Filtering df_raw using engine keywords
    df_engine = df_raw.filter(regex='|'.join(engine_search))

    # Adding engine dataframe to dictionary
    logs.update({'Engine': df_engine})

    # Flare keyword searches
    flare_search = ['Timestamp', 'Tag', 'Flare', 'flare', 'F1', 'Thermocouple']

    # Filtering df_raw using flare keywords
    df_flare = df_raw.filter(regex='|'.join(flare_search))

    # Adding flare dataframe to dictionary
    logs.update({'Flare': df_flare})

    # Add df for Four Hills Boiler
    if farm == 'Four Hills':
        boiler_search = ['Timestamp', 'Tag', 'Boiler']
        df_boiler = df_raw.filter(regex='|'.join(boiler_search))
        logs.update({'Boiler': df_boiler})

    print('')
    return logs

# Function to upload pandas dataframe to InfluxDB Server
# Accepts the name of the dataframe containing the data to be input, the name of the measurement (i.e. flare/engine/electricity) and the tag (i.e. project name)
def InfluxInput(df, measurement, bucket):
    # Setting the date_time column as the inplace index
    df.set_index(['Timestamp'], inplace=True)

    try:
        # Writing the dataframe to InfluxDB
        with InfluxDBClient(url=url, token=token, org=org) as client:
            with client.write_api(write_options=WriteOptions(batch_size=500,
                                                             flush_interval=10_000,
                                                             jitter_interval=2_000,
                                                             retry_interval=5_000,
                                                             max_retries=5,
                                                             max_retry_delay=30_000,
                                                             exponential_base=2)) as write_api:
                write_api.write(bucket, org, df, data_frame_measurement_name=measurement, data_frame_tag_columns=['Tag'])
        return True
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        return False

# Calling main function
main()
