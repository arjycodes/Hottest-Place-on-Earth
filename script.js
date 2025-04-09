async function fetchHottestPlace() {
    try {
        const response = await fetch('data.json?' + new Date().getTime());;
        const data = await response.json();
        updateDisplay(data);
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}

function updateDisplay(data) {
    document.getElementById('city').textContent = `${data.city}, ${data.country}`;
    document.getElementById('flag').src = `https://flagcdn.com/w160/${data.countryCode.toLowerCase()}.webp`;
    document.getElementById('flag').alt = `${data.country} flag`;
    document.getElementById('temperature').textContent = data.temperature;
    document.getElementById('condition').textContent = data.condition;
    document.getElementById('updated').textContent = `Last updated: ${data.lastUpdated}`;
    
    document.title = `${data.temperature} in ${data.city} - Hottest Place on Earth Right Now`;
}

// Initial fetch when page loads
fetchHottestPlace();

// Refresh data every 5 minutes
setInterval(fetchHottestPlace, 5 * 60 * 1000);
