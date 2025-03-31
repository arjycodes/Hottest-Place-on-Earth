import requests
from bs4 import BeautifulSoup
import json
import time

def scrape_hottest_place():
    url = "https://www.aqi.in/weather/live-ranking"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/',
            'DNT': '1',  # Do Not Track request header
            'Upgrade-Insecure-Requests': '1'
        }

        print("Making request to:", url)
        time.sleep(5)  # Delay to avoid triggering anti-scraping mechanisms

        response = requests.get(url, headers=headers)
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")

        if response.status_code != 200:
            print("Request failed! Check if the site is blocking GitHub Actions.")
            return False

        soup = BeautifulSoup(response.text, 'html.parser')

        # Debugging: Print part of the response to check structure
        print("Response Content Sample:", response.text[:300])

        location_entry = soup.find('a', class_='location')

        if not location_entry:
            print("Could not find location data. Possible changes in the website structure.")
            return False

        location_name_div = location_entry.find('div', class_='location-name')
        if not location_name_div:
            print("Could not find location-name div. Check site changes.")
            return False

        full_location = location_name_div.find('p', class_='font-bold').text.strip()

        if ',' in full_location:
            city, country = full_location.split(',', 1)
            city = city.strip()
            country = country.strip()
        else:
            city = full_location
            country = ""

        temp_div = location_entry.find('div', class_='temp-level')
        temperature = temp_div.find('span').text.strip() if temp_div else "N/A"

        condition_div = location_entry.find('div', class_='condition')
        condition = condition_div.find('p').text.strip() if condition_div else "N/A"

        last_updated_p = soup.find('p', class_='last-updated')
        last_updated = last_updated_p.text.replace('Last Updated:', '').strip() if last_updated_p else "N/A"

        flag_img = location_entry.find('img', alt=f"{country} flag")
        country_code = flag_img['src'].split('/')[-1].split('.')[0] if flag_img else "N/A"

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

    except Exception as e:
        print(f"Error scraping data: {e}")
        return False

if __name__ == "__main__":
    scrape_hottest_place()
