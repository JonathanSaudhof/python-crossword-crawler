import httpx 
import asyncio
from bs4 import BeautifulSoup
import json
import time
import os

from time import time, sleep
import uvloop
from json.decoder import JSONDecodeError
# garbage collector
import gc
uvloop.install()

baseUrl= "https://www.kreuzwort-raetsel.com"

progress = 0

# configuration of the http Client to prevent Timeouts and DDoS
limits = httpx.PoolLimits(max_keepalive=10, max_connections=20)
httpClient1 = httpx.AsyncClient(timeout=None, pool_limits=limits)


def getQuestionAnswers(html):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.tbody.findAll("tr")
    # answer result list
    r=[]
    # itterate throug the answers page and return the given answers
    for row in rows:
        rowTd = row.findAll("td")
        r.append(rowTd[0].a.string)
    return r


async def get_html(url):
    # TODO merge get answer and get_html to 
    res = await httpClient1.get(url)
    return res.text

async def get_answer(url, row):
    # TODO see get_html
    res = await httpClient1.get(url)
    # print(f'getHtml: {res.url}')
    return (res.text, row)

async def getPageQuestions(page, loop):
    # count itterations
    global progress
    # page result
    r = []
    # tasks list for the event loop
    tasks = []
    # timestamps to track duration / performance measurement
    tstart={}
    tend={}
    # request given page 
    print(f'page {page} requested!')
    html = await get_html("https://www.kreuzwort-raetsel.com/f/?Question_page=" + str(page))
    soup = BeautifulSoup(html, "lxml") 
    rows = soup.tbody.findAll("tr")
    # itterate throug page in the table to get all questions and links to answers
    for i,row in enumerate(rows):
        # get all data cells in row
        rowTd = row.findAll("td")
        # temp storage for not needed
        # rowData = {}
        # rowData["question"] = rowTd[0].a.string     

        # task for getting scraping the question related answers 
        # (passing the url an the id for the actual row / question of the page to merge it later on)
        task = loop.create_task(get_answer(baseUrl + rowTd[0].a.get("href"), i))
        # add task to the tasks list for the event loop
        tasks.append(task)
        # add question to the page result list 
        r.append({"question": rowTd[0].a.string  })
    # user feedback
    print(f'Awaiting Answers for Page: {page}')
    tstart[page] = time()
    # wait until all the tasks to be fullfilled
    answers = await asyncio.gather(*tasks)
    # itterate the answers and get the actual answer and the related row back
    for answer,row in answers:
        res = getQuestionAnswers(answer)
        # fill in the answer to the result page list
        r[row]["answers"] = res
    # open the output file to store the page into the file
    with open('./data_a.json', 'r+') as outfile:
        # read data from json
        data = json.load(outfile)
        # update data with page result
        data.update({page: r})
        # go back to the files beginning
        outfile.seek(0)
        # overwrite everything in the file with the updated data dict
        json.dump(data, outfile, indent=4)
        # close the file
        outfile.close
    # trying to release some memory (don't know if that works)
    gc.collect()
    # stop the time
    tend[page] = time()
    # increase the progress global 
    progress +=1
    # print out that everything worked out well
    print(f'Received Answers! Duration for page {page}: {round(tend[page] - tstart[page],2)}s | Progress: {progress}/{round(progress/1752*100,2)}%')

    
async def main():
    global progress
    tasks=[]
    loop = asyncio.get_event_loop()
    print('STARTED!')
    
    # open the data file to get all the pages which have been stored so far
    with open('data_a.json','r') as jsonfile:
        pages=json.load(jsonfile)
        jsonfile.close()
    
    for i in range(200,210):
        # if the requestsed page is already stored --> skip to the next
        if str(i) in pages:
            progress += 1
            print(f'Page {i} already exists |  Progress: {progress}/{round(progress/1752*100,2)}%')
        else:
            #if not, add getPageQuestion task for the given page to the tasks 
            tasks.append(loop.create_task(getPageQuestions(i,loop)))
            await asyncio.sleep(0.1)

    try:
        # start and await the event loop
        await asyncio.gather(*tasks)
    except:
        # in case of an error start again
        print(f'Error: start again!')
        sleep(3)
        await main()
    finally:  
        # if done, close the file and the http client
        jsonfile.close()
        await httpClient1.aclose()
        print('FINISHED!')
    
if __name__ == '__main__':
    asyncio.run(main())
