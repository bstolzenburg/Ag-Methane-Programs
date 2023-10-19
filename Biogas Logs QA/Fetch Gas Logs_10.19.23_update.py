import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import yaml
import re

## Setting Working Directory
# Get the directory where the script is located
dir_path = os.path.dirname(os.path.realpath(__file__))

# Change the current working directory to the script directory
os.chdir(dir_path)

# Get yaml file name
yaml_file = "Redlion_server_locations_5.18.23.yml"

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

            ## Filtering links by year

            # Initialize an empty dictionary to store the filtered links
            filtered_links_dict = {}

            # Iterate through the list of csv links to get the year from the url
            for link in csv_links:
                # Extract the year from the link
                match = re.search(r'/(\d{2})\d{6}\.CSV', link)

                # Check for matches
                if match:
                    year = f"20{match.group(1)}"

                    # Add the link to the dictionary corresponding to the year
                    if year in filtered_links_dict:
                        filtered_links_dict[year].append(link)
                    else:
                        filtered_links_dict[year] = [link]




            # ----------------------------------------------------------------
            ## Downloading the csv data and storing in a pandas dataframe

            # Iterate through each year in filtered_links_dict
            for year in filtered_links_dict:
                print('Downloading csv data for: ',year)
                print('')

                # Getting yearly csv links 
                year_csv_links = filtered_links_dict[year]
                
                # Initialize an empty list to store the indivudal weekly dataframes
                dfs = []

                # Initialize empty dictionary to store all yearly dataframes
                complete_dict = {}

                # Iterating through .csv links and downloading
                for link in year_csv_links[1:4]:    ## Only 1-4 for sake of testing to limit processing time
                    
                    # Getting weekly date from link, link specific
                    last_part = link.split('/')[-1]

                    # Splitting the date 
                    date = last_part.split('.')[0]
                    print('Downloading csv from: ',date)
                    print('')
                    
                    # Downloading csv data to temporary pandas dataframe
                    df_temp = download_logs(link,username,password)

                    # Error handling for empty dataframe 
                    if df_temp is not None: 
                        
                        # Appending dataframe to list of all dataframes from given year
                        dfs.append(df_temp)
                    else: 
                        print('Downloaded weekly flow log: ',date,' returned None. There was an error')
                        print('Proceeding to next link')



                # Concatenate all the dataframes from given year in into one dataframe
                appended_data = concatenate_dataframes(dfs)

                # Check if concatenate was successful 
                if appended_data is not None: 
                    print('Successfully concatenated the dataframes for ', year)
                    print('')

                    # If concat was successful, add yearly dataframe to dictionary

                    # Add dataframe to dictionary corresponding to given year key
                    complete_dict[year] = appended_data

                else: 
                    print('Error occurred during the concatenation for ',year )


            print('Finished adding all yearly dataframes to dictionary for ', project)
            print('')
            


            



                    


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



         

# Calling main function
main()


