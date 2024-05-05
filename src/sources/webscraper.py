
import requests
import re
from bs4 import BeautifulSoup


def get_fear_and_greed_vals(urls: list):

    bnb, cmc = urls[0], urls[1]
    
    html_doc = requests.get(bnb).text
    soup = BeautifulSoup(html_doc, 'html.parser')
    div = soup.find('div', class_='css-cxlpc6')
    bnb_idx = div.text.strip()

    html_doc = requests.get(cmc).text
    soup = BeautifulSoup(html_doc, 'html.parser')
    div = soup.find_all('span', class_=['sc-f70bb44c-0', 'ihcLcj', 'base-text'])

    for i in div:
        line = i.text.strip()
        match = re.match(r"\d+(?=/100)", line)
        if match:
            cmc_idx = match.group()
            try: 
                cmx_idx = int(match.group())
            except Exception as e:
                print(e)
                cmc_idx = "{not available}"
            break
        
    
    print(f'The Fear and Greed index for Binance: {bnb_idx} and Coin Market Cap: {cmc_idx}' ) #
    return bnb_idx, cmc_idx




if __name__=='__main__':

    urls = ["https://www.binance.com/en/square/fear-and-greed-index", "https://coinmarketcap.com/"]
    get_fear_and_greed_vals(urls)