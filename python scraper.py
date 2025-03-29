import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

def scrape_hottest_place():
    url = "https://www.aqi.in/weather/live-ranking"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        location_entry = soup.find('a', class_='location')
        
        if location_entry:
            location_name_div = location_entry.find('div', class_='location-name')
            full_location = location_name_div.find('p', class_='font-bold').text.strip()
            
            if ',' in full_location:
                city, country = full_location.split(',', 1)
                city = city.strip()
                country = country.strip()
            else:
                city = full_location
                country = ""
            
            temp_div = location_entry.find('div', class_='temp-level')
            temperature = temp_div.find('span').text.strip()
            
            condition_div = location_entry.find('div', class_='condition')
            condition = condition_div.find('p').text.strip()
            
            last_updated_p = soup.find('p', class_='last-updated')
            last_updated = last_updated_p.text.replace('Last Updated:', '').strip()
            
            flag_img = location_entry.find('img', alt=f"{country} flag")
            country_code = flag_img['src'].split('/')[-1].split('.')[0]
            
            data = {
                "city": city,
                "country": country,
                "countryCode": country_code,
                "temperature": temperature,
                "condition": condition,
                "lastUpdated": last_updated
            }
            
            with open('data.json', 'w') as f:
                json.dump(data, f, indent=2)
                
            print(f"Data updated: {city}, {country} at {temperature}")
            return True
        else:
            print("Could not find location data")
            return False
        
    except Exception as e:
        print(f"Error scraping data: {e}")
        return False

if __name__ == "__main__":
    scrape_hottest_place()
