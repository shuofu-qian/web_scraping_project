import requests
from bs4 import BeautifulSoup
import time

url = 'https://www.umei.cc/bizhitupian/diannaobizhi/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
resp = requests.get(url, headers = headers)

main_page = BeautifulSoup(resp.text, 'html.parser')
child_list = main_page.find_all('div',class_='item masonry_brick')
for child in child_list:
    child_url = 'https://www.umei.cc' + child.find('a').get('href')
    child_resp = requests.get(child_url, headers = headers)
    child_page = BeautifulSoup(child_resp.text, 'html.parser')
    img_url = child_page.find('div',class_='big-pic').find('img').get('src')

    img_resp = requests.get(img_url, headers = headers)
    img = img_resp.content
    img_name = img_url.split('/')[-1]

    with open('web_scraping_project/04.优美图库/img/'+img_name,'wb') as f:
        f.write(img)
    
    print('over!',img_name)

    time.sleep(1)

print('all over!')