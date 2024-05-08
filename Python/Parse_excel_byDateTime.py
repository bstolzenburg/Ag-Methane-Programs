# %%
import pandas as pd
from datetime import datetime
# This program takes in an excel file and splits the file into separate monthly files based on a datetime column
# It will create separate monthly files in the working directory using the extracted month

# Defining file name
file = r"LCFS Electricity Data\PG&E Data\Raw Files\11 PGE_Feb-Mar-24.xlsx"    # Modify file path as needed

# Reading in file
df = pd.read_excel(file)
print('Read in file: ',file)  

# %%
# Getting text string for the datetime column
date_time = 'elec_intvl_end_dttm'                                                       # Modify column name as needed

# Convert to date_time
df[date_time] = pd.to_datetime(df[date_time], format = '%m/%d/%Y %H:%M:%S')             # Modify format as needed

# Create year_month column
df['year_month'] = df[date_time].dt.to_period('M')


# %%
# Sorting by date_time
df.sort_values(by = date_time)

# %%
# Converting year_month to string 
df['year_month'].astype(str)

# %%
# Iterating through unique values and filtering dataframe 
for year_month in df['year_month'].unique():
    # Filtering dataframe 
    df_filt = df[df['year_month'] == year_month].copy()         

    # Dropping year_month column from df_filt
    df_filt.drop(columns = ['year_month'], inplace = True)

    ## Exporting filtered dataframe to excel 

    # Creating file name 

    # Creating string for file name using year_month variable
    name = datetime.strptime(str(year_month),'%Y-%m')

    # Formating datetime object as Month-Year
    new_name = name.strftime("%B-%y")

    # Creating file name
    file_name = f"PGE_{new_name}.xlsx"                             # Modify the file name as needed
    print(file_name)

    # Exporting data to excel files 
    print(f"Exporting: {new_name} \n")
    df_filt.to_excel(file_name, index = False)
    print(f"Successfully exported: {file_name} \n")

    

print('Finished')


