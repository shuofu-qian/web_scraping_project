import requests
import csv
import re
from bs4 import BeautifulSoup

url = 'https://movie.douban.com/chart'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
resp = requests.get(url, headers = headers)
# print(resp.text)

# re method
# 存在一部电影没有评分,可能会占用别的电影的评分,需要排除,有点麻烦
restr = re.compile(r'<table width="100%" class="">(?:(?!table).)*?title="(?P<name>(?:(?!table).)*?)">(?:(?!table).)*?<p class="pl">(?P<year>.{10})[(](?:(?!table).)*?'
                      r'="rating_nums">(?P<score>.*?)</span>.*?<span class="pl">[(](?P<num>.*?)人', re.S)
res = restr.finditer(resp.text)
with open('web_scraping_project/01.豆瓣/douban_re.csv',mode='w') as f:
    for i in res:
        name = i.group('name')
        year = i.group('year')
        score = i.group('score')
        num = i.group('num')
        csv.writer(f).writerow([name,year,score,num])


# BeautifulSoup method
page = BeautifulSoup(resp.text,'html.parser')
with open('web_scraping_project/01.豆瓣/douban_bs.csv',mode='w') as f:
    for i in page.find_all('table', width='100%',class_=''):
        # name = i.find('a',class_='').text.split('/')[0].strip()
        name = i.find('a',class_='nbg').get('title').strip('"')
        year = i.find('p',class_='pl').text[:10]
        score = i.find_all('span',class_='rating_nums')
        num = i.find('span',class_='pl').text[1:].strip(')人评价')

        if len(score) > 0: score = score[0].text
        else: score = None

        csv.writer(f).writerow([name,year,score,num])


resp.close()


    

