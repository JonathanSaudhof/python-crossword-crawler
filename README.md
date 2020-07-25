# python-crossword-crawler
My first attempt to get into python again after 3 years. 
The crawler gets all questions and related answers for a crossword game from a german site.

## Task:
Get all the 175 000+ questions and nested answers from a page to build a crossword riddle generator for fun. 
Do it as fast as possible! 

## Challenges:
1. Unfortunately I had to follow a nested link to get the answers. 
2. Started synchronously and moved to asyncio to improve the crawl speed
3. Struggled with connection loss due to server timeouts and DDoS prevention.
4. Dealed with caching the already crawled data to not crawl them again on a restart.

## Comment:
Was quite fun!

