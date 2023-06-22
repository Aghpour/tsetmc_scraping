import os
import pandas as pd
import requests
import time
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

industry_codes = []
for i in data:
    typee = i['type']
    code = i['code']
    if code<10:
        code = f'0{code}'
    if typee == 'IndustrialGroup':
        industry_codes.append(code)

industry_urls = []
for code in industry_codes:
    industry_url = f'http://cdn.tsetmc.com/api/ClosingPrice/GetRelatedCompany/{code}'
    industry_urls.append(industry_url)

# loop through industries urls
headers = {'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
tickers_code = []
for industry_url in industry_urls:
    response = requests.request("GET", industry_url, data=payload, headers=headers)
    industry_tickers = json.loads(response.text)['relatedCompany']
    for company in industry_tickers:
        insCode = company['instrument']['insCode']
        symbol = company['instrument']['lVal18AFC']
        if any(char.isdigit() for char in symbol): # drop debt instruments
            pass
        else:
            tickers_code.append(insCode)
####################################
dict_ticker = {}
interval = 4
max_calls = 6

@sleep_and_retry
@limits(calls=max_calls, period=interval)
def get_ticker(insCode):
    url_identity = f'http://cdn.tsetmc.com/api/Instrument/GetInstrumentIdentity/{insCode}'
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

shareholders = []
def shareholder(insCode):  
    url_shareholder = f'http://cdn.tsetmc.com/api/Shareholder/GetInstrumentShareHolderLast/{insCode}'
    r = s.get(url_shareholder, headers=headers)
    shareholder_page = json.loads(r.text)['shareHolder']
    df_shareholder = pd.DataFrame(shareholder_page)
    shareholders.append(df_shareholder)

with requests.Session() as s:
    for i, insCode in enumerate(tickers_code):
        get_ticker(insCode)
        try:
            shareholder(insCode)
        except:
            print(f'shareholder passed: {insCode}')
        
# info df
df_info = pd.DataFrame.from_dict(dict_ticker, orient='index', columns=['sector','name', 'en_company_id', 'ticker', 'exchange'])
df_info['en_instrument_id'] = df_info.index
df_info = df_info.reset_index(drop=True)
# shareholders df
df_shareholder = pd.concat(shareholders)
df_shareholder = df_shareholder.rename(columns={'cIsin':'en_company_id'})
# merge 2 df's
df_merged = pd.merge(df_shareholder, df_info, how='right', on = 'en_company_id')
#df_merged = df_merged.drop_duplicates(subset=['en_company_id', 'shareHolderID'], keep='first')
# export to cvs
df_merged.to_csv (r'share_holders.csv', index = False, header=True, sep ='\t')

end = time.time()
print("--- %0.1fs seconds ---" % (end - start))
