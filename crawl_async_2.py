import httpx 
import asyncio
from bs4 import BeautifulSoup
import json
import time

from time import time, sleep
import uvloop

import gc
uvloop.install()

baseUrl= "https://www.kreuzwort-raetsel.com"
calls = 0
progress = 0

limits = httpx.PoolLimits(max_keepalive=10, max_connections=20)
httpClient1 = httpx.AsyncClient(timeout=None, pool_limits=limits)


def getQuestionAnswers(html):
  
    soup = BeautifulSoup(html, "lxml")
    rows = soup.tbody.findAll("tr")

    r=[]
    for row in rows:
        rowTd = row.findAll("td")
        r.append(rowTd[0].a.string)
    return r


async def get_html(url):

    res = await httpClient1.get(url)
 

    # print(f'getHtml: {res.url}')
    return res.text

async def get_answer(url, row):

    res = await httpClient1.get(url)

    # print(f'getHtml: {res.url}')
    return (res.text, row)

async def getPageQuestions(page, loop):
    global result
    global progress
    r = []
    tasks = []
    with open('data_a.json','r') as jsonfile:
        
        data=json.load(jsonfile)
        jsonfile.close()
  
    if str(page) in data:
        
        progress += 1
        print(f'Page {page} already exists |  Progress: {progress}/{round(progress/1752*100,2)}%')
        return
    
    print(f'page {page} requested!')
    tstart={}
    tend={}
    
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
    tstart[page] = time()
    answers = await asyncio.gather(*tasks)
    
    progress +=1
  

    for answer,row in answers:
        res = getQuestionAnswers(answer)
        r[row]["answer"] = res
        # print(f'#{row} , {r[row]}' )
        
    gc.collect()
    with open('./data_a.json', 'r+') as outfile:
        data = json.load(outfile)
        data.update({page: r})
        outfile.seek(0)
        json.dump(data, outfile)
        outfile.close
        
    tend[page] = time()
    print(f'Received Answers! Duration for page {page}: {round(tend[page] - tstart[page],2)}s | Progress: {progress}/{round(progress/1752*100,2)}%')

    
async def main():
    tasks=[]
    loop = asyncio.get_event_loop()

    print('STARTED!')
      
    for i in range(1,1752):
        tasks.append(loop.create_task(getPageQuestions(i,loop)))
        if i % 10 == 0:
           await asyncio.sleep(1)

    try:
        await asyncio.gather(*tasks)
    except:
        print(f'Error: start again!')
        await main()
    finally:  
        await httpClient1.aclose()
        print('FINISHED!')
    
if __name__ == '__main__':
    asyncio.run(main())
