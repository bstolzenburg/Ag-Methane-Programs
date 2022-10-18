# Gas Log QA Script

# Version 1.4
# 8/16/2022
# Bryan Stolzenburg (Ag Methane Advisors)

# Iterate through list of farms
# Copy most recent merged_logs to the log_quick_qa for each farm
# and open the filled log_quick_qa.xlsx files to QA gaslogs after fetching

# List of Updates:
# 1.1 - Added user input to QA all farms or just specific farm(s)
# 1.2 - Added text wrapping and centering to first row for easier header reading
# 1.3 - Changed the format of the date column so excel will recognize as a date 
# 1.4 - Added function to automatically set working directory regardless of user




# Importing modules 
import openpyxl as xl
from pathlib import Path
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment, Font 
import pandas as pd
import os
import datetime 
from datetime import datetime



# Function to return file path to 'gaslogs' directory in dropbox to set working directory 
def SetDirectory():
    home = Path.home()

    gaslog_dir = os.path.join(home,'Patrick J Wood Dropbox','_operations','gaslogs')
    
    return(gaslog_dir)





# Defining function to get list of farms and create file paths to each farm's merged_log.csv file
def GetFarms():
    # Opening the txt list of farms
    farm_list = open('Farm_Names.txt','r')

    # Creating empty global list for farm names
    farms = []

    # Appending the names in the txt document to the global list of farm names for use in the main() function
    for line in farm_list.readlines(): 
        # Removing any empty lines 
        if not line.strip():
            continue
        else:
            # removing new line character and leading/trailing white space
            stripped_line = line.rstrip()
            stripped_line = stripped_line.strip()
            farms.append(stripped_line)

    # Closing the txt file 
    farm_list.close()

    # Get user input to QA specific farms 
    inputs = input('Do you want to QA all farms? (Y/N) ')
    print(' ')

    # Convert user input to lowercase 
    inputs = inputs.lower()

    if inputs == 'y':
        print('Running QA for all farms')
        print(' ')
    else:
        for (i,item) in enumerate(farms):
            print(i,item)
        print(' ')
        # Get user input(s) for farm number
        farm_index = list(map(int, input('Select a farm(s) from the above list using the corresponding number(s): ').split(' ')))
        
        # Selecting farms from master list based on farm_number input 
        farms = list(map(farms.__getitem__,farm_index))

    return(farms)




##### Defining main function to read files/ copy data to the quick_qa and open it

def main(): 

    # Setting working directory 
    os.chdir(SetDirectory())

    # Getting list of farm names and creating file paths
    farms = GetFarms()

    # Creating for loop to iterate through farms 
    for farm_name in farms: 
        
        #### Defining file/directory names for each farm
        name = farm_name
        
        # Creating name folder path
        file_name = farm_name + '_weekly'
        
        # Creating file path  
        file_path = os.path.join(SetDirectory(),file_name)

        # Creating csv names

        # Adding in the farms that need 'culled' added to the file path 
        if name == 'fourhills' or name == 'gallo' or name == 'Verweyhanford':
            csv = file_name + '_2022_merged_culled.CSV'
        else:
            csv = file_name + '_2022_merged.CSV'
        
        # creating raw quick_qa name 
        quick_qa = 'log_quick_qa_' + name + '.xlsx'

        # creating filled quick_qa name (this is the file that we will view, will be overwritten each time the code is run)
        quick_qa_filled = 'log_quick_qa_' + name + '_filled' '.xlsx'

        
        ### Loading Files

        # Changing the working directory using file_path variable
        os.chdir(file_path)

        print("Loading " + farm_name + " logs...")
        print(' ')

        # Reading the merged logs csv into a dataframe 
        logs = pd.read_csv(csv,encoding = 'ISO-8859-1',parse_dates = ['Date'])
        


        # Convert merged logs dataframe to rows (deletes the index column)
        rows = dataframe_to_rows(logs,index = False)


        # Load the log_quick_qa excel spreadsheet 
        qa = xl.load_workbook(quick_qa)
        sheet_qa = qa.active 
       
        
        

        

        ### Copying data from merged logs to quick_qa file 
        # Copy logs to qa spreadsheet 
        for rowidx, row in enumerate(rows,1): 
            for colidx, cell in enumerate(row,1): 
                sheet_qa.cell(row = rowidx, column = colidx, value = cell)

        

        # Adding format assigners for text wrapping and centering
        header_alignment = Alignment(wrap_text = True,vertical = 'center')
        
        # Applying formats to first row (headers)
        for cell in sheet_qa[1]:
            cell.alignment = header_alignment
            
            

        # Save the log_qa file 
        qa.save(quick_qa_filled)
        
        print('')
        input('Press Enter to Open QA/QC Spreadsheet: ')
        print('')
        
        # Open the log_qa_filled file 
        os.system(quick_qa_filled)

# Calling the main function 
main()

print('')
print('Finished')