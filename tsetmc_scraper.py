import asyncio
import aiohttp
import nest_asyncio
import time
import tsetmc
from tsetmc.instruments import Instrument
import pandas as pd

nest_asyncio.apply()
start = time.time()

df = pd.read_csv('tickers_urls.csv')
df_to_list = df['urls'].values.tolist()
ids = []
for i in df_to_list:
    ids.append(i[51:])
ids_len = len(ids)
    
ticker_data = []
identification = []
introduction = []
page = []
live = []
share_holders = []

async def main():
    async with aiohttp.ClientSession() as tsetmc.SESSION:
        counter = 0
        for url in ids:
            inst = Instrument(url)
            ##########################################
            iden = await inst.identification()
            identification.append(iden)
            ##########################################
            intro = await inst.introduction()
            intro['نماد فارسی'] = iden['نماد فارسی']
            introduction.append(intro)
            ##########################################
            page_data = await inst.page_data()
            page.append(page_data)
            ##########################################
            live_data = await inst.live_data()
            live.append(live_data)
            ##########################################
            shareholder = await inst.holders()
            shareholder['نماد فارسی'] = iden['نماد فارسی']
            share_holders.append(shareholder)
            ##########################################
            
            ticker_dict = {
            'کد 12 رقمی شرکت':iden['کد 12 رقمی شرکت'],
            'نام شرکت':iden['نام شرکت'],
            'کد 12 رقمی نماد':iden['کد 12 رقمی نماد'],
            'نماد فارسی':iden['نماد فارسی'],
            'بازار':iden['بازار'],
            'کد تابلو':iden['کد تابلو'],
            'گروه صنعت':iden['گروه صنعت'],
            ##########################################
            'تعداد سهام':page_data['z'],
            'حجم مبنا':page_data['bvol'],
            'درصد شناوری':page_data['free_float']
            }
            ##########################################
            intro_list = ['موضوع فعالیت','مدیر عامل', 'نشانی دفتر', 'نشانی' ,'نشانی امور سهام', 'حسابرس', 'سرمایه', 'سال مالی', 'شناسه ملی']
            for key in intro_list:
                try:
                    intro[key]
                except:
                    intro[key] = None
            
            ticker_dict['موضوع فعالیت'] = intro['موضوع فعالیت']
            ticker_dict['مدیر عامل'] = intro['مدیر عامل']
            ticker_dict['نشانی دفتر'] = intro['نشانی دفتر']
            ticker_dict['نشانی'] = intro['نشانی']
            ticker_dict['نشانی امور سهام'] = intro['نشانی امور سهام']
            ticker_dict['حسابرس'] = intro['حسابرس']
            ticker_dict['سرمایه'] = intro['سرمایه']
            ticker_dict['سال مالی'] = intro['سال مالی']
            ticker_dict['شناسه ملی'] = intro['شناسه ملی']
            ##########################################
            live_list = ['pc', 'status']
            for key in live_list:
                try:
                    live_data[key]
                except:
                    live_data[key] = None
            
            ticker_dict['قیمت پایانی'] = live_data['pc']
            ticker_dict['وضعیت'] = live_data['status']
            ##########################################
                
            ticker_data.append(ticker_dict)
            ticker_name = iden['نماد فارسی']
            counter += 1
            print(f'{counter} of {ids_len}, {ticker_name}')


asyncio.run(main())

end = time.time()
total_time = end - start
print("It took {} seconds to fetch {} tickers information.".format(total_time, len(ticker_data)))

df = pd.DataFrame.from_dict(ticker_data)
df.to_csv (r'ticker_data.csv', index = False, header=True, sep ='\t')

df_sh = pd.concat(share_holders)
df_sh.to_csv (r'share_holders.csv', index = False, header=True, sep ='\t')
