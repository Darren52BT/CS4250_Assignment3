from pymongo import MongoClient
from bs4 import BeautifulSoup
import re

#mongo connection to pages db
def connectDataBase():
    # Create a database connection object using pymongo
    try:
        client = MongoClient(host="localhost", port=27017)
        db = client.corpus
        print(client.list_database_names())

        return db
    except Exception as e:
        print(e)


#connect db and get professors collection, pages collections
db = connectDataBase()
professors = db.professors
pages = db.pages

#uses target page url to parse faculty members, uses professors collection to persist data
def parser():
    #use pages to get html based on target html
    page = pages.find_one({"_id": "https://www.cpp.edu/sci/computer-science/faculty-and-staff/permanent-faculty.shtml"})
    html = page.get('text')

    #create beautiful soup object
    bs = BeautifulSoup(html, 'html.parser')
    
    #professors section is under text-images section
    profsSection = bs.find('section', {'class':{'text-images'}})

    #the containers containing professors all have h2 elements, so get those h2 elements and grab their parents

    profHeaders = profsSection.find_all('h2')
    profContainers = list(map(lambda h2: h2.parent, profHeaders))

#for each professor
    for prof in profContainers:
        #get name, is simply the h2
        name = prof.h2.get_text().strip()

        #get title, next to strong element containing Title
        title = prof.find("strong", string=re.compile('(Title){1,1}')).next_sibling.get_text()
        #first char might have : so remove if so
        if (title[0] == ":"):
           title = title[1:]
        #trim
        title = title.strip()

        #get office, next to strong element containing Office
        office = prof.find("strong", string=re.compile('(Office){1,1}')).next_sibling.get_text()
        #first char might have : so remove if so
        if (office[0] == ":"):
           office = office[1:]
        #trim
        office = office.strip()

        #get email, href starts with mailto:
        email = prof.find('a', {'href': re.compile("^(mailto:)")}).get('href')
        #split by colon, get second part which is email address
        email=email.split(":")[1]

        #get website, is contained in anchor that starts with http(optional s)://www.cpp.edu/faculty
        website= prof.find('a', {'href':re.compile('^https{0,1}:\/\/www\.cpp\.edu\/faculty')}).get('href')


        #create prof document
        profDoc = {
            "_id": name,
            "name":name,
            "title":title,
            "office":office,
            "email":email,
            "website":website
        }

        #insert into mongo or update entry if it already exists

        currProf = professors.find_one({"_id":name})
        if (currProf):
            professors.update_one({"_id":name}, {"$set":profDoc})
        else:
            professors.insert_one(profDoc)


parser()