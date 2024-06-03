import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import yaml
import re
from pathlib import Path

## Setting Working Directory
# Get the directory where the script is located
dir_path = os.path.dirname(os.path.realpath(__file__))

# Change the current working directory to the script directory
os.chdir(dir_path)

# Get yaml file name
yaml_file = "Redlion_server_locations.yml"

## Main Function
def main():
    
    # Open yaml file to read contents
    with open(yaml_file, 'r') as f:
        # Load yaml file contents
        config = yaml.safe_load(f)

        # Iterating through each farm in YAML to extract project name, server location and authentication details
        for project, info in config.items():
            url = info['url']
            username = info['username']
            password = info['password']
            secondary = info['secondary']
            
            ## Creating a list of all csv url's from server for each farm. Complete list of all csvs avilable
            print("Downloading csv links for: ",project)
            print('')
            csv_links = get_links(url,username,password,secondary)
            
            # Iterating through csv_links to download the .csv from each link as a dataframe
            for link in csv_links[1:4]:                                                           # ONLY DOING 1:4 FOR TESTING
                print('Downloading data for', project,' from: ',link)
                print('')

                # Getting week date label from link string to create .csv name
                ## This will serve as the .csv file name
                csv_name = extract_week_number(link)

                # Getting date part of .csv name 
                date_part = csv_name.split('.')[0]

                # Extracting year, month and day from the date part
                year = int(date_part[:2])
                month = int(date_part[2:4])
                day = int(date_part[4:6])
                print(year,month,day)






                
                
                

                

                


            
            


# Functions ---------------------------------------------------------------

# Function to get complete links from url address
def get_links(url,username, password,secondary):
    # Create a requests session and authenticate with the credentials
    session = requests.Session()
    session.auth = (username, password)

    # Send an HTTP GET request to the URL and parse the HTML content
    response = session.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all the hyperlinks on the page and filter for CSV files
    links = soup.find_all('a')

    # Getting the .csv links (full path)
    csv_links_raw = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.CSV')]

    # Creating empty list to store the CSV links (relative paths)
    csv_links = []

    # Trimming .csv links
    for link in csv_links_raw: 
        # Finding last backslash in the link
        last_backslash = link.rfind('\\')

        # Get text after last backslash
        trimmed_link = link[last_backslash+1:]

        # Adding backslash to trimmed link
        trimmed_link = '/' + trimmed_link 
        

        # Appending the trimmed link to the list
        csv_links.append(trimmed_link)

    
    # For each of the csv links, combine it with the base url to get the full path to the data
    csv_links_full = []
    for link in csv_links: 
        if(secondary != 0):
            # Aurora ridge needs another 0 added to the link for it to be valid
            full_link = secondary + link
            csv_links_full.append(full_link)
        else:
            full_link = url + link
            csv_links_full.append(full_link)
    
    # Return complete list of csv links for given project
    return(csv_links_full)



# Function to downlod csv data from each weekly link 
def download_logs(csv_link, username, password):
    try:
        # Create a requests session and authenticate with the credentials
        session = requests.Session()
        session.auth = (username, password)

        # Send HTTP GET request to get the csv content from the link
        response = session.get(csv_link)

        # Check if the request was successful
        if response.status_code == 200:
            # Decode the csv content
            content = response.content.decode('latin1')

            # Read decoded content into dataframe
            df = pd.read_csv(StringIO(content))

            # Reverse the order of the dataframe
            df = df.iloc[::-1]

            return df
        else:
            print(f"Failed to retrieve data from {csv_link}. Status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return None

    except pd.errors.ParserError as e:
        print(f"An error occurred while parsing the CSV content: {e}")
        return None

# Function to concatenate logs into one main file    
def concatenate_dataframes(dfs):
    try:
        # Check if the list is not empty
        if len(dfs) == 0:
            raise ValueError("The list of dataframes is empty.")

        # Check if all dataframes have the same columns as the first dataframe
        columns_first_df = dfs[0].columns
        for i, df in enumerate(dfs[1:], start=1):
            if not all(df.columns == columns_first_df):
                print(f"DataFrame at index {i} has a different structure than the first dataframe.")
                print(f"Columns in DataFrame at index {i}: {df.columns}")

        # Concatenate all the dataframes in the list
        appended_data = pd.concat(dfs, ignore_index=True)

        return appended_data

    except ValueError as e:
        print(f"Error: {e}")


def extract_week_number(link):
    # Define regular expression for week sequence
    pattern = r'/(\d+)\.CSV'

    # Searching link for pattern 
    match = re.search(pattern,link)

    # Check for matches
    if match:
        # Return matched part of string (full string)
        full_string = match.group(1)

        # Removing last 2 zeros from string (so it is in YYMMDD format)
        string = full_string[:-2] + '.csv'
        
        return string
    else:
        return None
    

# Function to return file path to 'gaslogs' directory in dropbox to set working directory 
def GetDirectory(project):
    home = Path.home()

    gaslog_dir = os.path.join(home,'Patrick J Wood Dropbox','_operations','Gaslogs_new',project)
    
    return(gaslog_dir)



         

# Calling main function
main()


