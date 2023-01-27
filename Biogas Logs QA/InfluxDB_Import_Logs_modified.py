# Import Gas Flow Logs CSV to InfluxDB OSS Server
# Bryan Stolzenburg 
# Ag Methane Advisors
# 8/22/2022
# Version 1.4

## Updates: 
# Creating separate measurements for flare, engine data 
# Subsetting columns based on regular expressions that desribe specific devices
# Adding YAML file to store file paths to each farm's merged_logs.csv

# Importing Modules
import pandas as pd 
from pathlib import Path
import yaml
import os
import re


# Main Function
def main():

    # Setting directory to Github directory in the 'software' folder
    os.chdir(SetDirectory('software'))
    
   # Getting file locations from Gaslog_file_locations YAML file
    file_paths = GetFileLocations()


    # Setting directory to 'gaslogs' folder
    os.chdir(SetDirectory('gaslogs'))

    # Reading file_paths dictionary and calling the ProcessLogs() function to process the raw .csv logs and prepare for input to Influx server
    for farm,path in file_paths.items():
        
        # Calling ProcessLogs() function to return dictionary containing processed logs dataframes for each farm
        logs_dict = ProcessLogs(farm,path)

        for device,df in logs_dict.items():
            print(farm,device)
            print(df)





            









#### Functions: 



# Function to return unique file paths from home directory 
def SetDirectory(str):

    # Getting home path
    home = Path.home()
    
    # Getting path to directories
    if str == 'gaslogs':
        # Getting path for gaslogs directory
        gaslog_dir = os.path.join(home,'Patrick J Wood Dropbox','_operations','gaslogs')
        return(gaslog_dir)

    if str == 'software':
        # Getting path for softtware directory ############### CHANGE IF THE FILE IS MOVED TO GITHUB!!!!!!
        software_dir = os.path.join(home,'Patrick J Wood Dropbox','_operations','Software','Github','Ag-Methane-Programs','Biogas Logs QA')
        return(software_dir)

    if str == 'shortcut':
        # Getting path to shortcuts directory 
        shortcut_dir = os.path.join(home,'Patrick J Wood Dropbox','_operations','Software','Shortcuts')
        return(shortcut_dir)



# Function to read Gaslog_file_locations.yml and compile file locations into dictionary, returns dictionary with farm name as the key, file path as the value 
def GetFileLocations():

    
    # Setting file location for YAML config file 
    config = r'Gaslog_file_locations_23.yml'

    # Reading the YAML file and getting file paths dictionary 
    with open(config) as file:
        dict = yaml.load(file,Loader = yaml.FullLoader)
    
    # Creating blank dictionary to store file paths 
    file_paths = {}

    # Create filepaths for each farm 
    for farm,value in dict.items():
        # Create file path from value 
        file_path = os.path.join(value[0],value[1])
        # Append farm name and file path to dictionary
        file_paths.update({farm:file_path})

    # Return the dictionary
    return(file_paths)



    
    
# Function to read dictionary containing file paths for each farm, and to read the logs into separate pandas df by device 
# Returns dictionary containing dataframe for each device
def ProcessLogs(farm,path):

    print('Downloading logs for ',farm)
    print('')

    # Creating empty dictionary to store dataframes 
    logs = {}

    # Reading in the raw log .csv at the designated file path (taken from teh dictionary)
    df_raw = pd.read_csv(path,encoding = 'ISO-8859-1',parse_dates = {'Timestamp': ['Date','Time']},index_col = False)

    # Adding a tag column that is equal to the farm name 
    df_raw['Tag'] = farm

    # Fixing Hanford kWh output columns that have non-descriptive names 
    if farm == 'Hanford':
        df_raw.rename(columns = {'KWH Output':'Engine 1 KWH Output','KWH Output.1':'Engine 2 KWH Output',
        'KWH Output.2':'Engine 3 KWH Output'},inplace = True)



    ## Subsetting raw dataframe for columns related to each device
    
    # Engine keyword searches 
    engine_search = ['Timestamp','Tag','Engine','engine','G1','G2','G3','Gen']
    
    # Filtering df_raw using engine keywords
    df_engine = df_raw.filter(regex = '|'.join(engine_search))

    # Adding engine dataframe to dictionary 
    logs.update({'Engine':df_engine})

    # Flare keyword searches
    flare_search = ['Timestamp','Tag','Flare','flare','F1','Thermocouple']
    
    # Filtering df_raw using flare keywords
    df_flare = df_raw.filter(regex = '|'.join(flare_search))

    # Adding flare dataframe to dictionary 
    logs.update({'Flare':df_flare})

    # Add df for Four Hills Boiler
    if farm == 'Four Hills': 
        boiler_search = ['Timestamp','Tag','Boiler']
        df_boiler = df_raw.filter(regex = '|'.join(boiler_search))
        logs.update({'Boiler':df_boiler})
    
    # Call TotalHeaders function to get the headers that need to be parsed into flow/15 min
    for device,df in logs.items():

        # Getting list of totalizer headers for each device dataframe
        headers = TotalHeaders(farm,df) 
        print('Printing headers')
        print(headers)

        # Iterate through each column name in headers and calculating new column for 15 minute flow 
        for col_name in headers:
            print(col_name)
            # Creating new column name
            new_col = col_name + '_SCF_15min'

            # Calculating difference between consecutive totalizer values
            df.loc[:,new_col] = df.loc[:,col_name].diff(periods = -1)





    
    print('')
    
    return(logs)


# Function to get totalizer column header names so they can be operated on in ProcessLogs
def TotalHeaders(farm,device_df):
    # Getting indexes for totalizer columns
    df = device_df

    # Getting columns list from device df
    cols = list(df.columns)

    # Totalizer keyword searches
    totalizer_search = re.compile('Total')

    # Appending totalizer column headers to the totalizer_headers list
    cols = (list(filter(totalizer_search.search,cols)))
    
    return(cols)
    

# Running main function
main()





















