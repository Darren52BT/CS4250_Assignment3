from pymongo import MongoClient
import re 
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import deque 

#mongo connection to corpus db
def connectDataBase():
    # Create a database connection object using pymongo
    try:
        client = MongoClient(host="localhost", port=27017)
        db = client.corpus
        print(client.list_database_names())

        return db
    except Exception as e:
        print(e)


#crawler, takes in frontier and collection for data persistence
def crawlerThread(frontier: deque, collection):

    #stores visited urls
    visitedUrls = set()
    #while not frontier.done() do
    while len(frontier) > 0:
        #url <— frontier.nextURL()
        url = frontier.popleft()
        #add to visited
        visitedUrls.add(url)
        #html <— retrieveURL(url)
        #catch HTTP error, URLerror        
        try:
            html = urlopen(url)
        except HTTPError as e:
            print(e)
            visitedUrls.add(url)
            continue
        except URLError as e:
            print(e)
            visitedUrls.add(url)
            continue
            
        html = BeautifulSoup(html, 'html.parser')
        #persist data

        #if url already exists, update it
        #otherwise, insert

        page = collection.find_one({'_id': url})
        
        if(page):
            collection.update_one({'_id': url}, {"$set": {'text': str(html) }})
        else:
            collection.insert_one({'_id': url, 'text': str(html)})

       
        #if target_page (html) i.e. when crawler finds Permanent Faculty heading on page body
        if (html.find('h1', string=re.compile('(Permanent Faculty)+'))):
            #clear_frontier()
            frontier.clear()
        #else
        else:
            #first get all urls in html
            anchors = html.find_all('a')
            urls = []
            for anchor in anchors:
                href = anchor.get('href')
                urls.append(urljoin(url, href))
            #for each not visited url in parse (html) do
            for url in urls:
                if url not in visitedUrls:
                    #frontier.addURL(url)
                    frontier.append(url)
                    #visit url
                    visitedUrls.add(url)






#connect db
db = connectDataBase()

#get pages collection
pages = db.pages
#crawl and persist data
crawlerThread(deque(["https://www.cpp.edu/sci/computer-science/"]), pages)