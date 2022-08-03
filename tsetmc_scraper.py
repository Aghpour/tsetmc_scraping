import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import ast
from ratelimit import limits, RateLimitException, sleep_and_retry

start = time.time()
# find all listed tickers
url_marketwatch = 'http://tsetmc.com/tsev2/data/MarketWatchPlus.aspx'
r = requests.get(url_marketwatch)
soup = BeautifulSoup(r.content, 'html.parser')
data = soup.text.split(';')

# extract tickers code's, E.g.: http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={35425587644337450}
tickers_codes = []
for index, item in enumerate(data):
    if len(data[index].split(',')) == 8:
        tickers_codes.append(data[index].split(',')[0])
tickers_codes_unique = list(set(tickers_codes))
        
headers = {'User-Agent': '*put your user agent hear*'}
dict_ticker = {}
shareholders = []
interval = 4
max_calls = 10

@sleep_and_retry
@limits(calls=max_calls, period=interval)
def get_ticker(item):
    url_identity = f'http://cdn.tsetmc.com/api/Instrument/GetInstrumentIdentity/{item}'
    r = s.get(url_identity, headers=headers)
    identity_page = ast.literal_eval(r.text)  
    sector = identity_page['instrumentIdentity']['sector']['lSecVal']
    en_instrument_id = identity_page['instrumentIdentity']['instrumentID']  # unique
    name = identity_page['instrumentIdentity']['lSoc30']
    en_company_id = identity_page['instrumentIdentity']['cIsin']
    ticker = identity_page['instrumentIdentity']['lVal18AFC']
    exchange = identity_page['instrumentIdentity']['cgrValCotTitle']
    dict_ticker[en_instrument_id] = [sector, name, en_company_id, ticker, exchange]
    os.system('cls')
    print(i, ticker)
    
    
def shareholder(item):  
    url_shareholder = f'http://cdn.tsetmc.com/api/Shareholder/GetInstrumentShareHolderLast/{item}'
    r = s.get(url_shareholder, headers=headers)
    shareholder_page = ast.literal_eval(r.text)['shareHolder']
    df_shareholder = pd.DataFrame(shareholder_page)
    shareholders.append(df_shareholder)

with requests.Session() as s:
    for i, item in enumerate(tickers_codes_unique):
        get_ticker(item)
        try:
            shareholder(item)
        except:
            print(f'shareholder passed: {item}')
        
# info df
df_info = pd.DataFrame.from_dict(dict_ticker, orient='index', columns=['sector','name', 'en_company_id', 'ticker', 'exchange'])
df_info['en_instrument_id'] = df_info.index
df_info.reset_index(drop=True)
# shareholders df
df_shareholder = pd.concat(shareholders)
df_shareholder = df_shareholder.rename(columns={'cIsin':'en_company_id'})
# merge 2 df's
df_merged = pd.merge(df_shareholder, df_info, how='right', on = 'en_company_id')
df_merged = df_merged.drop_duplicates(subset=['en_company_id', 'shareHolderName'], keep='first')
# export to cvs
df_merged.to_csv (r'share_holders.csv', index = False, header=True, sep ='\t')

end = time.time()
print("--- %0.1fs seconds ---" % (end - start))
