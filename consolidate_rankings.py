import os
import glob
import pandas as pd
import re
from datetime import datetime

def consolidate_rankings_csv():
    # Define the input directory and output file
    data_dir = "Data"
    output_file = "consolidated_rankings.csv"
    
    # Check if the data directory exists
    if not os.path.exists(data_dir):
        print(f"Error: Directory '{data_dir}' not found.")
        return False
    
    # Find all CSV files in the data directory
    csv_files = glob.glob(os.path.join(data_dir, "rankings_*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in '{data_dir}' directory.")
        return False
    
    print(f"Found {len(csv_files)} CSV files to consolidate.")
    
    # Initialize an empty list to store all dataframes
    all_data = []
    
    # Process each CSV file
    for file in csv_files:
        try:
            # Extract datetime from filename using regex
            filename = os.path.basename(file)
            match = re.search(r'rankings_(\d{8})_(\d{6})\.csv', filename)
            
            if match:
                date_str, time_str = match.groups()
                # Parse the datetime
                scraped_datetime = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                
                # Read the CSV file
                df = pd.read_csv(file)
                
                # Add the scraped_datetime column
                df['scraped_datetime'] = scraped_datetime
                
                # Append to the list of dataframes
                all_data.append(df)
                print(f"Processed: {filename}")
            else:
                print(f"Skipping file with invalid naming format: {filename}")
        
        except Exception as e:
            print(f"Error processing file {file}: {str(e)}")
    
    if not all_data:
        print("No valid data found to consolidate.")
        return False
    
    # Combine all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Sort the data by scraped_datetime and rank
    combined_df.sort_values(by=['scraped_datetime', 'rank'], ascending=[True, True], inplace=True)
    
    # Drop duplicates based on scraped_datetime and rank (keeping the first occurrence)
    combined_df.drop_duplicates(subset=['scraped_datetime', 'rank'], keep='first', inplace=True)
    
    # Save the consolidated data to CSV
    combined_df.to_csv(output_file, index=False)
    
    print(f"Consolidation complete. {len(combined_df)} rows written to {output_file}")
    print(f"Data spans from {combined_df['scraped_datetime'].min()} to {combined_df['scraped_datetime'].max()}")
    
    return True

if __name__ == "__main__":
    consolidate_rankings_csv()
