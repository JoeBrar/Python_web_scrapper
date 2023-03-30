import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import json
import sqlite3

url = "https://www.theverge.com/"
response = requests.get(url)
html = response.content

soup = BeautifulSoup(html, "html.parser")

jsonscript=soup.find('script',{'id':'__NEXT_DATA__'})
str2=str(jsonscript)
str2=str2.replace('<script id="__NEXT_DATA__" type="application/json">', '')
str2=str2.replace('</script>', '')

json_object = json.loads(str2)

prettyJson = json.dumps(json_object, indent=1, separators=(',', ': '))

articlesContainer=json_object["props"]["pageProps"]["hydration"]["responses"][0]["data"]["community"]["frontPage"]["placements"]
data=[]

for item in articlesContainer:
    if item["placeable"]:
        if item["placeable"]["type"]=='STORY':
            headline=item["placeable"]["title"]
            author=item["placeable"]["author"]["fullName"]
            articleUrl=item["placeable"]["url"]
            fulldatetime=item["placeable"]["publishDate"]
            articleDate=fulldatetime.split('T')[0]
            print(headline+"\n"+author+"\n"+articleDate+"\n")
            data.append([articleUrl, headline, author, articleDate])

today = datetime.date.today().strftime("%d%m%Y")

df = pd.DataFrame(data, columns=["URL", "headline", "author", "date"])
df.insert(0,'id', df.index+1 )

filename=today+"_verge.csv"
df.to_csv(filename, index=False)

conn = sqlite3.connect('theverge.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS articles
             (id INTEGER PRIMARY KEY, URL TEXT, headline TEXT, author TEXT, date TEXT)''')

for row in data:
    c.execute("INSERT INTO articles (url, headline, author, date) VALUES (?, ?, ?, ?)", row)


#remove the duplicate rows
duplicate_query = """
SELECT URL, COUNT(*)
FROM articles
GROUP BY URL
HAVING COUNT(*) > 1
"""
c.execute(duplicate_query)
duplicates = c.fetchall()

for duplicate in duplicates:
    url = duplicate[0]
    count = duplicate[1]
    delete_query = f"""
    DELETE FROM articles
    WHERE URL = "{url}"
    AND id NOT IN (
        SELECT id FROM articles WHERE URL = "{url}" LIMIT 1
    )
    """
    c.execute(delete_query)


conn.commit()
conn.close()