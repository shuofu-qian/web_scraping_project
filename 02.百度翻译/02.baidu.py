import requests

# 注意这里的网址后面有sug
url = 'https://fanyi.baidu.com/sug'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
dat = {'kw': 'dog'}
resp = requests.post(url, headers = headers, data=dat)

print(resp.json())

