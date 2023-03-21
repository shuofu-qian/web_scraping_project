#!/usr/bin/env python
# coding: utf-8

# In[4]:

import sys
import json
import time
import random
import requests
import aiohttp
import asyncio
import aiomysql
import pymysql
import argparse
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime


def getShopId():
    conn = pymysql.connect(
        host="101.43.231.237",
        port=3306,
        user="user_qihang",
        passwd="M7zSrg44cWF8lzo9",
        database="173_test_com"
    )

    df = pd.read_sql('''SELECT shopId, dpUrl from dp_basic_data;''', conn) # 获取数据
    df = df.sort_values('shopId') # 按照点评shopId排序
    shopid_li = df.shopId.tolist() # 获取点评shopId
    sid_li = df.dpUrl.str.rsplit('/').str.get(-1).tolist() # 获取点评爬虫url的shopId
    shopid_dict = dict(zip(sid_li, shopid_li)) # 拼接为dict
    conn.close()  # Close the database connection
    
    return sid_li, shopid_li, shopid_dict


def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))
    
    
async def getPost(sid, pool):
    async with pool.acquire() as conn:
        cursor = await conn.cursor()

        url_web = 'http://m.dianping.com/shop/' + sid
        url_1 = f'https://mapi.dianping.com/mapi/msource/shop.bin?device_system=MACINTOSH&lat=0&lng=0&mtsiReferrer=pages%2Fdetail%2Fdetail%3FshopUuid%3D{sid}%26online%3D1%26shopuuid%3D{sid}%26shopId%3D{sid}%26pageName%3Dshop&online=1&pageName=shop&shopUuid={sid}&shopuuid={sid}&'
        url_2 = f'https://m.dianping.com/wxmapi/shop/rankinfo?device_system=MACINTOSH&shopUuid={sid}&showPos=tag&'
        headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url_1, headers=headers) as resp1, session.get(url=url_2, headers=headers) as resp2:
                    if (resp1.status == 200) and (resp2.status == 200):
                        data = await resp1.json()
                        rank = await resp2.json()
                        
                        values = (
                            shopid_dict[sid],
                            url_web,
                            data['name'],
                            data['branchName'],
                            data['shopPowerRate'],
                            data['scoreText'],
                            data['priceText'],
                            data['voteTotal'],
                            data['cityName'],
                            data['regionName'],
                            data['address'],
                            data['recentBizTime']['title'] if data['recentBizTime'] is not None else '',
                            rank['rankInfo']['rankShortName'] if rank['code'] == 200 else '',
                            rank['rankInfo']['rankings'] if rank['code'] == 200 else 0,
                            rank['rankInfo']['rankUrl'] if rank['code'] == 200 else '',
                            datetime.now().strftime('%Y-%m-%d')
                        )

                        print(values, end='\n\n')

                        await cursor.execute("""
                            INSERT IGNORE INTO shopWebData
                            (shopID, url, name, branchName, shopPowerRate, scoreText,
                            priceText, voteTotal, cityName, regionName, address,
                            recentBizTime, rankShortName, rankings, rankUrl, date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, values)

                        await conn.commit()

        except Exception as e:
            print("getPost:", e)
            print(sid, "restarts")
            await getPost(sid, pool)

            
async def getPostProxy(sid, pool):
    async with pool.acquire() as conn:
        cursor = await conn.cursor()

        proxy = get_proxy().get("proxy")
        print(proxy)
        url_web = 'http://m.dianping.com/shop/' + sid
        url_1 = f'https://mapi.dianping.com/mapi/msource/shop.bin?device_system=MACINTOSH&lat=0&lng=0&mtsiReferrer=pages%2Fdetail%2Fdetail%3FshopUuid%3D{sid}%26online%3D1%26shopuuid%3D{sid}%26shopId%3D{sid}%26pageName%3Dshop&online=1&pageName=shop&shopUuid={sid}&shopuuid={sid}&'
        url_2 = f'https://m.dianping.com/wxmapi/shop/rankinfo?device_system=MACINTOSH&shopUuid={sid}&showPos=tag&'
        headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url_1, proxy=f'http://{proxy}', headers=headers) as resp1, session.get(url=url_2, proxy=f'http://{proxy}', headers=headers) as resp2:
                    if (resp1.status == 200) and (resp2.status == 200):
                        data = await resp1.json()
                        rank = await resp2.json()
                        
                        values = (
                            shopid_dict[sid],
                            url_web,
                            data['name'],
                            data['branchName'],
                            data['shopPowerRate'],
                            data['scoreText'],
                            data['priceText'],
                            data['voteTotal'],
                            data['cityName'],
                            data['regionName'],
                            data['address'],
                            data['recentBizTime']['title'] if data['recentBizTime'] is not None else '',
                            rank['rankInfo']['rankShortName'] if rank['code'] == 200 else '',
                            rank['rankInfo']['rankings'] if rank['code'] == 200 else 0,
                            rank['rankInfo']['rankUrl'] if rank['code'] == 200 else '',
                            datetime.now().strftime('%Y-%m-%d')
                        )

                        print(values, end='\n\n')

                        await cursor.execute("""
                            INSERT IGNORE INTO shopWebData
                            (shopID, url, name, branchName, shopPowerRate, scoreText,
                            priceText, voteTotal, cityName, regionName, address,
                            recentBizTime, rankShortName, rankings, rankUrl, date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, values)

                        await conn.commit()

        except Exception as e:
            print("getPost:", e)
            print(sid, "restarts")
            await getPost(sid, pool)


async def getPosts(sid_li, start_id, end_id):  
    pool = await aiomysql.create_pool(
        host='101.43.231.237',
        port=3306,
        user='user_qihang',
        password='M7zSrg44cWF8lzo9',
        db='DZDPdata',
        minsize=1,
        maxsize=10000
    )
    
    try:
        if get_post_method == 'y':
            tasks = [asyncio.create_task(getPostProxy(sid, pool)) for sid in sid_li[start_id: end_id + 1]]  
        elif get_post_method == 'n':
            tasks = [asyncio.create_task(getPost(sid, pool)) for sid in sid_li[start_id: end_id + 1]]    
        await asyncio.wait(tasks) 
        
    except Exception as e:
        print("getPosts:", e)
        
    pool.close()
    await pool.wait_closed()

    
async def main(args, speed = 100):
    
    global sid_li, shopid_li, shopid_dict, get_post_method
    
    get_post_method = args.arg1

    sid_li, shopid_li, shopid_dict = getShopId()
    
    for i in tqdm(range(0, len(sid_li), speed)):
        t2 = datetime.now()
        time.sleep(5)
        await getPosts(sid_li, i, i + speed - 1)
        print("Num:", min(len(sid_li), i + speed), "/", len(sid_li), "ETA:", (datetime.now() - t2) / speed * max(0, (len(sid_li) - i - speed)))   
    
    print("Done!") 

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Use local machine or proxies to get data from dianping.com")
    parser.add_argument("-proxy", "--arg1", default='n', type=str, help="whether or not use free proxies? [y/n]")

    args = parser.parse_args()
    asyncio.run(main(args))

