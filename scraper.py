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
    Append the data to consolidated_rankings.csv with a timestamp column
    """
    if df.empty:
        print("No data to save")
        return
    
    # Add scraped_datetime column with current timestamp
    current_time = datetime.now(timezone.utc)
    df['scraped_datetime'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Path to the consolidated CSV file
    csv_path = "consolidated_rankings.csv"
    
    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(csv_path)
    
    # If file exists, read it and append new data
    if file_exists:
        existing_df = pd.read_csv(csv_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df
    
    # Sort by scraped_datetime and rank
    combined_df = combined_df.sort_values(by=['scraped_datetime', 'rank'])
    
    # Save to CSV
    combined_df.to_csv(csv_path, index=False)
    
    print(f"Data appended to {csv_path}")
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
