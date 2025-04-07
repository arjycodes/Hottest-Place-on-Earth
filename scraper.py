import requests
import json
import re
import pandas as pd
import time
import random
import os
from datetime import datetime, timezone
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# New imports
import duckdb
import boto3
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from io import BytesIO

def get_consolidated_data(source: str = 'local') -> pd.DataFrame:
    """
    Retrieve consolidated data from the specified source.
    """
    if source == 'local':
        csv_path = "consolidated_rankings.csv"
        if os.path.isfile(csv_path):
            return pd.read_csv(csv_path)
        else:
            return pd.DataFrame()
    elif source == 'r2':
        # Implement R2 DuckDB file retrieval logic here
        pass
    else:
        raise ValueError(f"Unsupported data source: {source}")

def save_to_duckdb(df: pd.DataFrame, connection: duckdb.DuckDBPyConnection) -> None:
    """
    Save DataFrame to DuckDB.
    """
    connection.execute("CREATE TABLE IF NOT EXISTS rankings AS SELECT * FROM df")
    connection.execute("INSERT INTO rankings SELECT * FROM df")

def upload_to_r2(local_path: str, bucket_name: str, object_name: str) -> None:
    """
    Upload a file to Cloudflare R2.
    """
    s3 = boto3.client('s3',
                      endpoint_url=os.environ['R2_ENDPOINT'],
                      aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'])
    
    s3.upload_file(local_path, bucket_name, object_name)

def upload_to_google_drive(file_path: str, file_name: str, mime_type: str) -> None:
    """
    Upload a file to Google Drive.
    """
    creds = Credentials.from_authorized_user_file('google_credentials.json', ['https://www.googleapis.com/auth/drive.file'])
    service = build('drive', 'v3', credentials=creds)

    file_metadata = {'name': file_name}
    media = MediaIoBaseUpload(BytesIO(open(file_path, 'rb').read()), mimetype=mime_type, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()



def create_session() -> requests.Session:
    """
    Creates a requests session with retry capabilities and 
    anti-scraping measures
    """
    # Setup session with retry strategy
    session = requests.Session()
    
    # Configure retry strategy for failed requests
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    # Mount the adapter to the session
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Choose a random user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
    ]
    user_agent = random.choice(user_agents)
    
    # Set common headers
    session.headers.update({
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "DNT": "1",  # Do Not Track
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    })
    
    return session

def scrape_temperature_rankings() -> List[Dict[str, Any]]:
    """
    Scrapes temperature rankings data from AQI.in website using anti-scraping techniques
    """
    url = "https://www.aqi.in/weather/live-ranking"
    
    try:
        # Create session with anti-scraping measures
        session = create_session()
        
        # Add random delay to mimic human behavior
        time.sleep(random.uniform(1, 3))
        
        # Make the request
        response = session.get(url)
        response.raise_for_status()
        page_source = response.text
        
        marker = "\"rankings\\\":"
        start_idx = page_source.find(marker)
        
        if start_idx == -1:
            print("Rankings marker not found in the page source")
            return []
            
        array_start = page_source.find('[', start_idx)
        if array_start == -1:
            print("Could not find start of rankings array")
            return []
            
        bracket_count = 1
        pos = array_start + 1
        
        while pos < len(page_source) and bracket_count > 0:
            char = page_source[pos]
            if char == '[':
                bracket_count += 1
            elif char == ']':
                bracket_count -= 1
            pos += 1
            
        if bracket_count != 0:
            print("Failed to find the complete rankings array")
            return []
            
        json_array = page_source[array_start:pos]
        try:
            json_array = json_array.replace('\\"', '"').replace('\\\\', '\\')
            rankings_data = json.loads(json_array)
            print(f"Successfully extracted {len(rankings_data)} rankings")
            return rankings_data
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            return []
            
    except Exception as e:
        print(f"Error: {e}")
        return []

def process_rankings(rankings: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Process rankings data and return a DataFrame with all relevant fields.
    """
    if not rankings:
        return pd.DataFrame()
    
    processed_data = []
    
    for item in rankings:
        record = item.copy()  # Start with all top-level fields
        
        # Process weather data
        if 'weather' in record:
            weather = record.pop('weather')
            for key, value in weather.items():
                if key != 'condition':
                    record[key] = value
                else:
                    # Only keep the 'text' from condition, discard 'icon'
                    record['condition_text'] = value.get('text')
        
        # Extract country code from flag URL
        if 'flag' in record:
            flag_url = record.pop('flag')
            country_code = flag_url.split('/')[-1].split('.')[0]
            record['country_code'] = country_code
        
        processed_data.append(record)
    
    return pd.DataFrame(processed_data)

def save_top_rank_data(rankings: List[Dict[str, Any]]) -> None:
    """
    Save the number 1 ranked city data to data.json in the root folder
    """
    if not rankings or len(rankings) == 0:
        print("No rankings data available to create top rank data")
        return
    
    # Get the top ranked city (rank 1)
    top_rank = next((city for city in rankings if city.get('rank') == 1), rankings[0])
    
    # Format the data according to the specified structure
    current_time = datetime.now(timezone.utc).strftime("%d %b %Y, %I:%M %p")
    
    # Extract country code from flag URL
    country_code = "unknown"
    if 'flag' in top_rank:
        flag_url = top_rank.get('flag', '')
        country_code = flag_url.split('/')[-1].split('.')[0]
    
    # Get temperature with degree symbol
    temp_c = "N/A"
    if 'weather' in top_rank and 'temp_c' in top_rank['weather']:
        temp_c = f"{top_rank['weather']['temp_c']} \u00b0C"
    
    # Get condition text
    condition = "Unknown"
    if 'weather' in top_rank and 'condition' in top_rank['weather']:
        condition = top_rank['weather']['condition'].get('text', 'Unknown')
    
    top_rank_data = {
        "city": top_rank.get('city', 'Unknown'),
        "country": top_rank.get('country', 'Unknown'),
        "countryCode": country_code,
        "temperature": temp_c,
        "condition": condition,
        "lastUpdated": current_time
    }
    
    # Save to data.json in the root folder
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(top_rank_data, f, indent=2, ensure_ascii=False)
    
    print("Top rank data saved to data.json")

def save_data(df: pd.DataFrame) -> None:
    """
    Save data to local CSV, DuckDB (local, R2), and Google Drive.
    """
    if df.empty:
        print("No data to save")
        return

    # Add scraped_datetime column with current timestamp
    current_time = datetime.now(timezone.utc)
    df['scraped_datetime'] = current_time.strftime("%Y-%m-%d %H:%M:%S")

    # Local CSV - primary storage that should always work
    csv_path = "consolidated_rankings.csv"
    existing_df = get_consolidated_data()
    combined_df = pd.concat([existing_df, df], ignore_index=True)
    combined_df = combined_df.sort_values(by=['scraped_datetime', 'rank'])
    combined_df.to_csv(csv_path, index=False)
    print(f"Data successfully saved to local CSV: {csv_path}")

    # Try to save to local DuckDB
    try:
        with duckdb.connect('rankings.db') as conn:
            save_to_duckdb(combined_df, conn)
        print("Data successfully saved to local DuckDB")
    except Exception as e:
        print(f"Failed to save to local DuckDB: {e}")

    # Try to upload to R2
    try:
        upload_to_r2('rankings.db', 'weather-rankings-data', 'rankings.db')
        print("Data successfully uploaded to R2")
    except Exception as e:
        print(f"Failed to upload to R2: {e}")

    # Try to upload DuckDB to Google Drive
    try:
        upload_to_google_drive('rankings.db', 'rankings.db', 'application/octet-stream')
        print("DuckDB file successfully uploaded to Google Drive")
    except Exception as e:
        print(f"Failed to upload DuckDB to Google Drive: {e}")

    # Try to upload CSV to Google Drive
    try:
        upload_to_google_drive(csv_path, 'consolidated_rankings.csv', 'text/csv')
        print("CSV backup successfully uploaded to Google Drive")
    except Exception as e:
        print(f"Failed to upload CSV to Google Drive: {e}")

    print(f"\nDataset Summary:")
    print(f"- Total rankings in this scrape: {len(df)}")
    print(f"- Total rankings in consolidated file: {len(combined_df)}")
    print(f"- Columns: {', '.join(df.columns.tolist()[:10])}...")

def main() -> None:
    """Main function to execute the scraping process"""
    print("Starting weather rankings data collection...")
    
    # Add a random delay before starting
    time.sleep(random.uniform(1, 3))
    
    # Scrape with exponential backoff if needed
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        rankings = scrape_temperature_rankings()
        
        if rankings:
            # Save the top rank data first
            save_top_rank_data(rankings)
            
            # Process and save the full dataset
            df = process_rankings(rankings)
            save_data(df)
            
            print("Data collection completed successfully")
            break
        else:
            if attempt < max_attempts:
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Attempt {attempt} failed. Waiting {wait_time:.2f} seconds before retry...")
                time.sleep(wait_time)
            else:
                print("All attempts failed. Could not retrieve rankings data.")

if __name__ == "__main__":
    main()
