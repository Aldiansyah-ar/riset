from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
user_agent = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}

def detik_page(query, start_date, end_date):
  global user_agent
  url = f'https://www.detik.com/search/searchnews?query={query}&siteid=3&sortby=time&sorttime=1&fromdatex={start_date}&todatex={end_date}&result_type=latest'
  text = requests.get(url, user_agent).text
  sop = BeautifulSoup(text, 'lxml')
  try:
    paging = sop.find_all('div','pagination text-center mgt-16 mgb-48')[0].find_all('a')[-2]
    last_page = paging.text
  except:
    last_page = 1
  return last_page

def save_to_mongodb(data, db_config, collection_name):
   client = MongoClient(db_config['host'], db_config['port'])
   db = client[db_config['db_name']]
   collection = db[collection_name]

   if data:
      collection.insert_many(data)
   client.close()

def scrape_detik(query, start_date, end_date, db_config, collection_name):
    global user_agent
    last_page = detik_page(query, start_date, end_date)
    data = []
    id = 0
    for page in range(1,int(last_page)+1):
        url = f'https://www.detik.com/search/searchnews?query={query}&siteid=3&sortby=time&sorttime=1&fromdatex={start_date}&todatex={end_date}&result_type=latest&page={page}'
        text = requests.get(url,user_agent).text
        soup = BeautifulSoup(text, 'lxml')
        articles_container = soup.find_all('article', class_='list-content__item')
        print(f'page: {page}')
        for article in articles_container:
            headline = article.find('div', 'media__text').find('a').text
            link = article.find('div', 'media__text').find('a')['href']
            link_ = link + f'?single=1'
            try:
              news = requests.get(link_,user_agent).text
              news_soup = BeautifulSoup(news, 'lxml')
              date = news_soup.find('div', class_='detail__date').text
              content_container = news_soup.find('div', class_='detail__body-text itp_bodycontent')
              location = content_container.find('strong').text
              contents = content_container.find_all('p', class_='')
              text = []
              id += 1
              # content = content_container.text.replace('ADVERTISEMENT','').replace('SCROLL TO RESUME CONTENT','').replace('SCROLL TO CONTINUE WITH CONTENT', '').replace('\n\n\r\n\r\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\r\n\r\n\n','').replace('\n\n\n','').replace('\n\n','')
              for x in contents:
                  y = x.text
                  text.append(y)
                  content = ' '.join(text).replace('ADVERTISEMENT','').replace('SCROLL TO RESUME CONTENT','').replace('SCROLL TO CONTINUE WITH CONTENT', '')

              content = location + ' - ' + content
              print(f'article: {id}')

              data.append({'_id': id,
                          'source': 'Detik',
                          'title': headline,
                          'url': link,
                          'content': content,
                          'date': date})
            except:
              pass

    # Simpan ke database
    save_to_mongodb(data, db_config, collection_name)

# Konfigurasi database MongoDB
db_config = {
    'db_name': 'covidnews',
    'host': 'localhost',
    'port': 27017
}

# Contoh pemanggilan fungsi
scrape_detik('Kecelakaan Cibubur', '18/07/2022', '24/07/2022', db_config, 'kecelakaancibubur')