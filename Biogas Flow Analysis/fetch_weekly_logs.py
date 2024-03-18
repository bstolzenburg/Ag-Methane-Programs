# Adjusting the script to accept dates in MM/DD/YYYY format

import os
import shutil
from datetime import datetime
from glob import glob

# Function to request and validate user input for dates, accepting MM/DD/YYYY format
def request_and_validate_dates():
    date_format = "%m/%d/%Y"
    while True:
        start_date_str = input("Enter the start date (MM/DD/YYYY): ")
        end_date_str = input("Enter the end date (MM/DD/YYYY): ")
        try:
            start_date = datetime.strptime(start_date_str, date_format).date()
            end_date = datetime.strptime(end_date_str, date_format).date()
            if start_date <= end_date:
                return start_date, end_date
            else:
                print("The start date must be before the end date. Please try again.")
        except ValueError:
            print("Invalid date format. Please use MM/DD/YYYY.")

# Function to find files within the date range, now handling the input date format conversion
def find_files_in_range(start_date, end_date, source_directory):
    date_format_file = "%y%m%d"  # File names are in YYMMDD format
    files_in_range = []
    for file_path in glob(os.path.join(source_directory, "*.CSV")):
        file_name = os.path.basename(file_path)
        file_date_str = file_name[:6]
        try:
            file_date = datetime.strptime(file_date_str, date_format_file).date()
            if start_date <= file_date <= end_date:
                files_in_range.append(file_path)
        except ValueError:
            continue  # Skip files that don't match the expected naming convention
    return files_in_range

# Function to copy files to a new directory, remains unchanged
def copy_files_to_new_directory(files, destination_directory):
    os.makedirs(destination_directory, exist_ok=True)
    for file_path in files:
        shutil.copy(file_path, destination_directory)

# Adjusted main function to reflect the new date input format
def main():
    start_date, end_date = request_and_validate_dates()
    source_directory = r'ENTER FILE PATH'                                                # UPDATE FILE PATH

    # Get the directory where the script is located
    dir_path = os.path.dirname(os.path.realpath(__file__))

    # Change the current working directory to the script directory
    os.chdir(dir_path)

    # Create destination file path directory for retrieved logs
    destination_directory = os.path.join(os.getcwd(), "Weekly Logs")

    files_in_range = find_files_in_range(start_date, end_date, source_directory)
    copy_files_to_new_directory(files_in_range, destination_directory)
    print(f"Copied {len(files_in_range)} files to {destination_directory}")


main()


