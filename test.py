import multiprocessing 
import requests
from bs4 import BeautifulSoup
import json
result={}
result["data"]=[]
baseUrl: str = "https://www.kreuzwort-raetsel.com"
global progress
progress = 0
# print("progress 1", progress)

def getQuestionAnswers(url):
    html = requests.get(url).content
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


def getPageQuestions(page):
    # global progress
    # progress+=1
    # print("progress 3", progress)

  
    html = requests.get("https://www.kreuzwort-raetsel.com/f/?Question_page=" + str(page)).content
    soup = BeautifulSoup(html, "lxml") 
    rows = soup.tbody.findAll("tr")
    r = []
    for row in rows:
        rowTd = row.findAll("td")
        data = {}
        data["question"] = ("").join(rowTd[0].a.contents)
        data["answers"] = getQuestionAnswers(baseUrl + rowTd[0].a.get("href"))
        r.append(data)
    # print(r)
    result["data"].append(r)
    


if __name__ == '__main__':
    # print("progress 2", progress)
    pool = multiprocessing.Pool()
    pool.map(getPageQuestions, range(1,1751))
    pool.close()
    with open('data.json', 'w') as outfile:
        json.dump(result, outfile)