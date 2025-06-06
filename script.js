// Add this to your script.js file
document.addEventListener('touchmove', function(e) {
    // Check if the scroll position is at the top
    if (window.scrollY === 0) {
      // Allow scrolling (for pull-to-refresh) only when at the top
      return;
    }
    // Otherwise prevent scrolling
    e.preventDefault();
  }, { passive: false });

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
    // Update content on the page
    document.getElementById('city').textContent = `${data.city}, ${data.country}`;
    document.getElementById('flag').src = `https://flagcdn.com/w320/${data.countryCode.toLowerCase()}.webp`;
    document.getElementById('flag').alt = `${data.country} flag`;
    document.getElementById('temperature').textContent = data.temperature;
    document.getElementById('condition').textContent = data.condition;
    document.getElementById('updated').textContent = `Last updated: ${data.lastUpdated} UTC`;

    // Update the page title
    // document.title = `${data.temperature} in ${data.city} - The Hottest Place on Earth Right Now`;

    // Dynamically generate the weather icon URL
    const weatherIcon = document.getElementById('weather-icon');
    const baseURL = "https://www.aqi.in/media/weather-icons/";
    const conditionSlug = data.condition.toLowerCase().replace(/\s+/g, "-"); // Convert condition to lowercase slug
    weatherIcon.src = `${baseURL}${conditionSlug}.svg`; // Build the dynamic URL
    weatherIcon.alt = `${data.condition} icon`; // Set the alt attribute

    weatherIcon.onerror = () => {
        weatherIcon.src = `${baseURL}sunny.svg`; // Default icon
    };
    
}

// Initial fetch when page loads
fetchHottestPlace();

// Refresh data every 5 minutes
setInterval(fetchHottestPlace, 5 * 60 * 1000);
