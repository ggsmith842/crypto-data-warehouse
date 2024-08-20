import requests

HOST = "https://v2.api.carbon-ratings.com/"

def get_emissions_data(ticker: str, key: str) -> requests.request:
    
    url = f"{HOST}currencies/{ticker}/emissions/network?key={key}"
    response = requests.request("GET", url).json()

    return response

def get_netork_power_consumption(ticker: str, key: str):
    url = f"{HOST}currencies/{ticker}/power/network?key={key}"

    response = requests.request("GET", url).json()
    return response

def get_netork_power_consumption(ticker: str, key: str):
    url = f"{HOST}currencies/{ticker}/electricity-consumption/network?key={key}"

    response = requests.request("GET", url).json()
    return response


# def parse_json_to_table():



      

    