import tabula
import pandas as pd
import PyPDF2
import re
import os

# Setting directory to file location
dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir)

# Define the PDF file path
pdf_path = 'GHCND_USW00093242_2024-1-1.pdf'

# Use tabula to extract the table from the PDF
tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)

# Extract the relevant table (assuming the desired data is in the first table)
if tables:
    # Getting first table extractd
    df = tables[0]

    # Print the columns to understand the structure
    # print("Extracted Columns:")
    # print(df.columns)
    # print("Extracted DataFrame:")
    # print(df.head())

    # UNCOMMENT ABOVE TO CHECK STRUCTURE OF EXTRACTED DATA
    
    # Attempt to locate the relevant columns (e.g., Month and Mean)
    # Adjust based on actual column names found
    try:
        df_filtered = df[['Month', 'Mean']]
    except KeyError:
        # If exact columns are not found, adjust accordingly (e.g., by inspecting column names)
        df_filtered = df.iloc[:, [0, 1]]  # Assuming the first two columns are Month and Mean
    
    # Extract the year from the text
    year = "Year not found"

    # Extracting year from below "Global Summary of the Month"
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        year_match = re.search(r'Global Summary of the Month\s+for\s+(\d{4})', text)
        if year_match:
            year = year_match.group(1)

    # Print the extracted information
    print(f"Year: {year}")
else:
    print("No tables found in the PDF.")


# Extracting correct rows from table (starting with Jan)
df_table = df_filtered.iloc[10:]

# Naming columns
df_table.columns = ['month','mean']


# Creaing date column 
df_table['month'] = pd.to_datetime(df_table['month'] + ' ' + str(year), format = '%b %Y') + pd.offsets.MonthBegin(0)

print(df_table)