import httpx 
import multiprocessing 
import asyncio
from bs4 import BeautifulSoup
import requests
import json
import time
import logging
from time import time, sleep
import uvloop
from random import random
from aiohttp import ClientSession
import aiohttp
from random import *
uvloop.install()
result={}
result["data"]=[]
baseUrl= "https://www.kreuzwort-raetsel.com"
calls = 0
progress = 0

limits = httpx.PoolLimits(max_keepalive=10, max_connections=50)
httpClient1 = httpx.AsyncClient(timeout=None, pool_limits=limits)
httpClient2 = httpx.AsyncClient(timeout=None, pool_limits=limits)
httpClient3 = httpx.AsyncClient(timeout=None, pool_limits=limits)
httpClient4 = httpx.AsyncClient(timeout=None, pool_limits=limits)
# print("progress 1", progress)

def getQuestionAnswers(html):

    soup = BeautifulSoup(html, "lxml")
    rows = soup.tbody.findAll("tr")

    r=[]
    for row in rows:
        rowTd = row.findAll("td")
        r.append(rowTd[0].a.string)
    return r



async def get_html(url):
    rand = randint(1,4)
    if rand == 1 :
        res = await httpClient1.get(url)
    if rand == 2 :
        res = await httpClient2.get(url)
    if rand == 3:
        res = await httpClient3.get(url)
    if rand == 4:
        res = await httpClient4.get(url)

    # print(f'getHtml: {res.url}')
    return res.content

async def get_answer(url, row):
    rand = randint(1,4)
    if rand == 1 :
        res = await httpClient1.get(url)
    if rand == 2 :
        res = await httpClient2.get(url)
    if rand == 3:
        res = await httpClient3.get(url)
    if rand == 4:
        res = await httpClient4.get(url)
 
    # print(f'getHtml: {res.url}')
    return (res.content, row)

async def getPageQuestions(page, loop):
    global result
    r = []
    tasks = []

    # for n in range(until):
    #     task = asyncio.ensure_future(get_html("https://www.kreuzwort-raetsel.com/f/?Question_page=" + str(n)))
    #     tasks.append(task)
    #     time.sleep(0.5)
    
    # questions = await asyncio.gather(*tasks)
     
    #    print(questions)
    tstart={}
    tend={}
    tstart[page] = time()
    html = await get_html("https://www.kreuzwort-raetsel.com/f/?Question_page=" + str(page))
    soup = BeautifulSoup(html, "lxml") 
    rows = soup.tbody.findAll("tr")
    tasks =[]
    for i,row in enumerate(rows):
        rowTd = row.findAll("td")
        data = {}
        data["question"] = rowTd[0].a.string      
        task = loop.create_task(get_answer(baseUrl + rowTd[0].a.get("href"), i))
        tasks.append(task)
        r.append(data)

    print(f'Awaiting Answers for Page: {page}')
    answers = await asyncio.gather(*tasks)
    print(f'Received Answers for Page: {page}')

    for answer,row in answers:
        res = getQuestionAnswers(answer)
        r[row]["answer"] = res
        # print(f'#{row} , {r[row]}' )
        
   
    result['data'].extend(r)
    tend[page] = time()
    print(f'duration for page {page}: {tend[page] - tstart[page]}s')
    print(f'result-data length at page {page}: {len(result["data"])}')




    
async def main():
    tasks=[]
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)

  
    for i in range(1,1752):
        tasks.append(loop.create_task(getPageQuestions(i,loop)))

    await asyncio.gather(*tasks)
    await httpClient.aclose()
    with open('data_a.json', 'w') as outfile:
        json.dump(result, outfile)
    

    
if __name__ == '__main__':
    asyncio.run(main())
