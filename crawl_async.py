import httpx 
import asyncio
from bs4 import BeautifulSoup
import requests
import json
import time
import logging
from time import time, sleep

result={}
result["data"]=[]
baseUrl= "https://www.kreuzwort-raetsel.com"
calls = 0
progress = 0
# print("progress 1", progress)

def getQuestionAnswers(html):

    soup = BeautifulSoup(html, "lxml")
    rows = soup.tbody.findAll("tr")

    r=[]
    for row in rows:
        rowTd = row.findAll("td")
        r.append(rowTd[0].a.string)
    return r



async def get_page(url, page):
    limits = httpx.PoolLimits(max_keepalive=5, max_connections=10)
    client = httpx.AsyncClient(timeout=None, pool_limits=limits)
    await asyncio.sleep(0.1)
    print(f'scheduled page {page}')
    try:
        res = await client.get(url)
    finally:
        await client.aclose()
    # print(f'getHtml: {res.url}')
    return (res.text, page)

async def get_answer(url, page, row):
    limits = httpx.PoolLimits(max_keepalive=5, max_connections=10)
    client = httpx.AsyncClient(timeout=None, pool_limits=limits)
    asyncio.sleep(0.09)
    print(f'scheduled Answer for page {page} row {row}')
    try:
        res = await client.get(url)
    finally:
        await client.aclose()
    return (res.text, page, row)

async def getPageQuestions(until):
    r = []
    tasks = []
    
    for page in range(until):
        task = asyncio.ensure_future(get_page("https://www.kreuzwort-raetsel.com/f/?Question_page=" + str(page),page))
        tasks.append(task)
       
    
    pages = await asyncio.gather(*tasks)
    questionAnswers = {}
    #    print(questions)
    answerTasks = []
    for html,page in pages:
        
        soup = BeautifulSoup(html, "lxml") 
        questions = soup.tbody.findAll("tr")
        
        questionAnswers[page]=[]
        for i, question in enumerate(questions):
            rowTd = question.findAll("td")
            data ={}
            data["question"] = rowTd[0].a.string
            questionAnswers[page].append(data)
            answerTask = asyncio.ensure_future(get_answer(baseUrl + rowTd[0].a.get("href"), page, i))
            answerTasks.append(answerTask)
    print(len(answerTasks))
    answers = await asyncio.gather(*answerTasks)

    for answer,page,i in answers:
        res = getQuestionAnswers(answer)
        questionAnswers[page][i]["answers"] = res
        # print(f'#{row} , {r[row]}' )
        # answers = await asyncio.gather(*tasks2)

        # for i, html in enumerate(answers):
        #     r[i]["answer"] = getQuestionAnswers(html)
        
    # TODO erge data together in an object

    # print(questionAnswers)
    with open('data_a.json', 'w') as outfile:
        json.dump(questionAnswers, outfile)


    
def main():
    t0=time()
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    future = asyncio.ensure_future(getPageQuestions(1752))
    loop.run_until_complete(future)
    
    t1=time()

    print(f"lasted time: {(t1-t0)}s")
    print(f"data length{len(result['data'])}")

    

async def distributor(executor, sites):
    log = logging.getLogger('run_blocking_tasks')
    log.info('starting')
    
    log.info('creating executor tasks')
    loop = asyncio.get_event_loop()
    blocking_tasks = [
        loop.run_in_executor(executor, main, i)
        for i in range(6)
    ]

if __name__ == '__main__':
    main()
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format='%(threadName)10s %(name)18s: %(message)s',
    #     stream=sys.stderr,
    # )
    
    # executor = concurrent.futures.ThreadPoolExecutor(
    #     max_workers=12,
    # )
    
    # event_loop = asyncio.get_event_loop()

    # try:
    #     event_loop.run_until_complete(
    #         distributor(executor, 1752)
    #     )
    # finally:
    #     event_loop.close()