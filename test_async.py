import aiohttp 
import asyncio
from bs4 import BeautifulSoup
import json
import time
import uvloop
uvloop.install()

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
        data = {}
        data["answer"] = ("").join(rowTd[0].a.contents)
        data["characters"] = int(rowTd[1].contents[0].split()[0])
        r.append(data)
    return r



async def get_html(url, session, sem):
    async with sem:
        async with session.get(url) as resp:
         
            html = await resp.text()
            return html


async def getPageQuestions(until):
    r = []
    tasks = []
    sem = asyncio.Semaphore(30)
    async with aiohttp.ClientSession() as session1:
        for n in range(until):
            print(n)
            task = asyncio.ensure_future(get_html("https://www.kreuzwort-raetsel.com/f/?Question_page=" + str(n), session1, sem))
            tasks.append(task)
            await asyncio.sleep(0.0)
        
        questions = await asyncio.gather(*tasks)
    
    print(questions)
    
    answers=[]
    async with aiohttp.ClientSession() as session2:
        for html in questions:
            soup = BeautifulSoup(html, "lxml") 
            rows = soup.tbody.findAll("tr")
           
            for row in rows:
                print(row)
                rowTd = row.findAll("td")
                answers.append(asyncio.ensure_future(get_html(baseUrl + rowTd[0].a.get("href"), session2)))   
        answers = await asyncio.gather(*answers)

    # TODO erge data together in an object
    result["data"].append(r)



    
def main():

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    future = asyncio.ensure_future(getPageQuestions(1751))
    loop.run_until_complete(future)
    print(result)
    with open('data_a.json', 'w') as outfile:
        json.dump(result, outfile)
    
    print("Done.")

if __name__ == '__main__':

    main()