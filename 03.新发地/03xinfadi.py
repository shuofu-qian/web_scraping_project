import requests
import pandas as pd


url = 'http://www.xinfadi.com.cn/getPriceData.html'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
dat = { 'limit': 20,
        'current': 1,  # 此为页码
        'pubDateStartTime': '', 
        'pubDateEndTime': '',
        'prodPcatid': '',
        'prodCatid': '',
        'prodName': ''}
resp = requests.post(url, headers=headers, data=dat)
list = resp.json().get('list')

pd.DataFrame(list).to_csv('web_scraping_project/03.新发地/xinfadi.csv')

