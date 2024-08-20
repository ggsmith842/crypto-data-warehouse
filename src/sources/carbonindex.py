import requests


def get_emissions_data(ticker: str, key: str):
    
    url = "https://v2.api.carbon-ratings.com/currencies/{ticker}/emissions/network?key={key}"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)

      

    