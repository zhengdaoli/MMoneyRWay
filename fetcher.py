import requests

def fetch_exchange_rates(api_key):
    base_url = "https://openexchangerates.org/api/latest.json"
    params = {
        "app_id": api_key
    }

    response = requests.get(base_url, params=params)

    # Ensure the request was successful
    response.raise_for_status()

    data = response.json()
    return data["rates"]

if __name__ == "__main__":
    API_KEY = "YOUR_OPEN_EXCHANGE_RATES_API_KEY"  # Replace with your API key
    rates = fetch_exchange_rates(API_KEY)
    for currency, rate in rates.items():
        print(f"1 USD = {rate} {currency}")

