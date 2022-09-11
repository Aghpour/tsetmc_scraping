import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import ast
import json
from ratelimit import limits, RateLimitException, sleep_and_retry

start = time.time()
####################################
# extract industry code
url = 'http://cdn.tsetmc.com/api/StaticData/GetStaticData'

payload = ''
headers = {'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}

response = requests.request("GET", url, data=payload, headers=headers)
data = json.loads(response.text)['staticData']

industry_code = {}
for i in data:
    typee = i['type']
    name = i['name'].strip().replace('\u200c', '')
    code = i['code']
    if typee == 'IndustrialGroup':
        industry_code[name] = code
        
urls = []
for key, value in industry_code.items():
    url = f'http://cdn.tsetmc.com/api/ClosingPrice/GetRelatedCompany/{value}'
    urls.append(url)

# loop through industries urls
tickers_code = []
tickers = {}
for url in urls:
    response = requests.request("GET", url, data=payload, headers=headers)
    ticker_data = json.loads(response.text)['relatedCompany']
    for i in ticker_data:
        insCode = i['instrument']['insCode']
        ticker_name = i['instrument']['lVal18AFC']
        tickers_code.append(insCode)
        #tickers[ticker_name] = insCode
####################################
        
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
dict_ticker = {}
shareholders = []
interval = 4
max_calls = 6

@sleep_and_retry
@limits(calls=max_calls, period=interval)
def get_ticker(item):
    url_identity = f'http://cdn.tsetmc.com/api/Instrument/GetInstrumentIdentity/{item}'
    r = s.get(url_identity, headers=headers)
    identity_page = json.loads(r.text)  
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
    shareholder_page = json.loads(r.text)['shareHolder']
    df_shareholder = pd.DataFrame(shareholder_page)
    shareholders.append(df_shareholder)

with requests.Session() as s:
    for i, item in enumerate(tickers_code):
        get_ticker(item)
        try:
            shareholder(item)
        except:
            print(f'shareholder passed: {item}')
        
# info df
df_info = pd.DataFrame.from_dict(dict_ticker, orient='index', columns=['sector','name', 'en_company_id', 'ticker', 'exchange'])
df_info['en_instrument_id'] = df_info.index
df_info = df_info.reset_index(drop=True)
# shareholders df
df_shareholder = pd.concat(shareholders)
df_shareholder = df_shareholder.rename(columns={'cIsin':'en_company_id'})
# merge 2 df's
df_merged = pd.merge(df_shareholder, df_info, how='right', on = 'en_company_id')
df_merged = df_merged.drop_duplicates(subset=['en_company_id', 'shareHolderID'], keep='first')
# export to cvs
df_merged.to_csv (r'share_holders.csv', index = False, header=True, sep ='\t')

end = time.time()
print("--- %0.1fs seconds ---" % (end - start))
