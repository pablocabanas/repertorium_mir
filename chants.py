# imports
import sys
import requests
import re
import pickle
import os
import urllib.parse
from bs4 import BeautifulSoup
import sqlite3
from tqdm import tqdm

# url constants
CANTUS_URL = 'https://cantusindex.org'
CANTUS_LIST_URL = 'https://cantusindex.org/chants'
GENRE_LIST_URL = 'https://cantusindex.org/genre'
FEAST_LIST_URL = 'https://cantusindex.org/feasts'
STORAGE_FOLDER = './storage/'
CANTUS_LIST_FILE = STORAGE_FOLDER + 'chant_list.pkl'
GENRE_LIST_FILE = STORAGE_FOLDER + 'genre_list.pkl'
FEAST_LIST_FILE = STORAGE_FOLDER + 'feast_list.pkl'
CANTUS_DB_FILE = STORAGE_FOLDER + 'chant_info.db'

# session handler
session = requests.Session()
session.headers.update(
    {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0'
    }
)


def cantus_list_chants():
    """Retrieve the list of all chants in CantusIndex
    """
    # load locally stored list
    if os.path.exists(CANTUS_LIST_FILE):
        with open(CANTUS_LIST_FILE, 'rb') as archivo:
            chants = pickle.load(archivo)
        return chants

    # fetch first page
    page = session.get(CANTUS_LIST_URL)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # retrieve number of pages
    li = parsed.find('li', class_='pager-last last')
    href = li.find('a').get('href')
    num_pages = int(re.search(r'page=(\d+)', href).group(1))

    chants = []

    # iterate over pages
    for p in range(0, num_pages + 1):
        print(f"Reading page {p}/{num_pages}")

        page = session.get(CANTUS_LIST_URL + '?page=' + str(p))
        parsed = BeautifulSoup(page.content, 'html.parser')

        # process table and add chants
        table = parsed.find('table', class_='views-table cols-5').find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            chants.append(row.find('a').get('href'))

    os.makedirs(STORAGE_FOLDER, exist_ok=True)
    with open(CANTUS_LIST_FILE, 'wb') as archivo:
        pickle.dump(chants, archivo)

    return chants


def cantus_list_genres():
    """Retrieve the list of all genres in CantusIndex
    """
    # load locally stored list
    if os.path.exists(GENRE_LIST_FILE):
        with open(GENRE_LIST_FILE, 'rb') as archivo:
            genres = pickle.load(archivo)
        return genres
    
    # fetch genre table
    page = session.get(GENRE_LIST_URL)
    parsed = BeautifulSoup(page.content, 'html.parser')

    genres = []

    # process table
    table = parsed.find('table').find('tbody')
    rows = table.find_all('tr')
    for row in rows:
        genres.append(row.find('a').get('href'))

    os.makedirs(STORAGE_FOLDER, exist_ok=True)
    with open(GENRE_LIST_FILE, 'wb') as archivo:
        pickle.dump(genres, archivo)

    return genres


def cantus_list_feasts():
    """Retrieve the list of all feasts in CantusIndex
    """
    # load locally stored list
    if os.path.exists(FEAST_LIST_FILE):
        with open(FEAST_LIST_FILE, 'rb') as archivo:
            feasts = pickle.load(archivo)
        return feasts
    
    # fetch feast table
    page = session.get(FEAST_LIST_URL)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # retrieve number of pages
    href = parsed.find('a', {'title': 'Go to last page'}).get('href')
    num_pages = int(re.search(r'page=(\d+)', href).group(1))

    feasts = []

    # iterate over pages
    for p in range(0, num_pages + 1):
        page = session.get(FEAST_LIST_URL + '?page=' + str(p))
        parsed = BeautifulSoup(page.content, 'html.parser')

        table = parsed.find('table').find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            feasts.append(row.find('a').get('href'))

    os.makedirs(STORAGE_FOLDER, exist_ok=True)
    with open(FEAST_LIST_FILE, 'wb') as archivo:
        pickle.dump(feasts, archivo)

    return feasts


def get_chant(chanturl):
    """Retrieve chant info
    """
    # get chant page
    page = session.get(CANTUS_URL + '/' + chanturl)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # get CantusID, fulltext, genre, feast, source, cao, cao concordances
    cantusid = urllib.parse.unquote(chanturl.split('/')[-1])

    try:
        fulltext = parsed.find('div', string='Full text:\xa0').\
            find_next('div', {'class': 'field-item even'}).text
    except:
        fulltext = ''

    try:
        genre = parsed.find('div', string='Genre:\xa0').\
            find_next('a').get('href').split('/')[-1]
        genre = urllib.parse.unquote(genre)
    except:
        genre = ''

    try:
        feast = parsed.find('div', string='Feast:\xa0').\
            find_next('a').get('href').split('/')[-1]
        feast = urllib.parse.unquote(feast)
    except:
        feast = ''

    try:
        source = parsed.find('div', string='Fulltext source:\xa0').\
            find_next('div', {'class': 'field-item even'}).text
    except:
        source = ''

    try:
        cao = parsed.find('div', string='CAO:\xa0').\
            find_next('div', {'class': 'field-item even'}).text
    except:
        cao = ''

    try:
        caoc = parsed.find('div', string='CAO concordances:\xa0').\
            find_next('div', {'class': 'field-item even'}).text
        caoc = " ".join(caoc.split())
    except:
        caoc = ''

    return {
        'id': cantusid,
        'text': fulltext,
        'genre': genre,
        'feast': feast,
        'source': source,
        'cao': cao,
        'caoc': caoc,
        }


def get_genre(genreurl):
    """Retrieve genre info
    """
    # get genre page
    page = session.get(CANTUS_URL + '/' + genreurl)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # get id, name, description, office, rite
    id = urllib.parse.unquote(genreurl.split('/')[-1])

    try:
        name = parsed.find('h1', {'class': 'page-title'}).text
    except:
        name = ''

    try:
        desc = parsed.find('div', {'class': 'views-field views-field-description'})
        desc = desc.find('p').text
    except:
        desc = ''
    
    try:
        office = parsed.find('div', {'class': 'views-field views-field-field-mass-office'})
        office = office.find('div', {'class': 'field-content'}).text
    except:
        office = ''

    try:
        rite = parsed.find('div', {'class': 'views-field views-field-field-rite'})
        rite = rite.find('div', {'class': 'field-content'}).text
    except:
        rite = ''
    
    return {
        'id': id,
        'name': name,
        'desc': desc,
        'office': office,
        'rite': rite,
        }


def get_feast(feasturl):
    """Retrieve feast info
    """
    # get genre page
    page = session.get(CANTUS_URL + '/' + feasturl)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # get id, name, description, date
    id = urllib.parse.unquote(feasturl.split('/')[-1])

    try:
        name = parsed.find('h1', {'class': 'page-title'}).text
    except:
        name = ''

    try:
        desc = parsed.find('div', {'class': 'views-field views-field-description'})
        desc = desc.find('p').text
    except:
        desc = ''

    try:
        date = parsed.find('div', {'class': 'views-field views-field-field-feast-date'})
        date = date.find('div', {'class': 'field-content'}).text
    except:
        date = ''

    return {
        'id': id,
        'name': name,
        'desc': desc,
        'date': date,
        }


def create_db_tables():
    """Create database tables
    """
    # connect (or create) to database
    conection = sqlite3.connect(CANTUS_DB_FILE)

    cursor = conection.cursor()

    # create genre table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS genre (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        desc TEXT NOT NULL,
        office TEXT NOT NULL,
        rite TEXT NOT NULL
    )
    ''')

    # create feast table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS feast (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        desc TEXT NOT NULL,
        date TEXT NOT NULL
    )
    ''')

    # create chant table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chant (
        id TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        genre TEXT,
        feast TEXT,
        source TEXT NOT NULL,
        cao TEXT NOT NULL,
        caoc TEXT NOT NULL,
        FOREIGN KEY (genre) REFERENCES genre (id),
        FOREIGN KEY (feast) REFERENCES feast (id)
    )
    ''')

    conection.commit()
    conection.close()


def get_chant_all(chants):
    """Retrieve and store info from all chants
    """
    # connect to database
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()

    # process all chants
    for chant in tqdm(chants, file=sys.stdout):

        # check whether this chant is already in DB
        id = urllib.parse.unquote(chant.split('/')[-1])
        cursor.execute('SELECT id FROM chant WHERE id = ?', (id,))
        result = cursor.fetchall()
        if len(result) > 0:
            continue

        chantinfo = get_chant(chant)
        cursor.execute('''
            INSERT INTO chant (id, text, genre, feast, source, cao, caoc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (chantinfo['id'], 
                  chantinfo['text'], 
                  chantinfo['genre'],
                  chantinfo['feast'],
                  chantinfo['source'],
                  chantinfo['cao'],
                  chantinfo['caoc'])
        )
    conection.commit()
    conection.close()


def get_genre_all(genres):
    """Retrieve and store info from all genres
    """
    # connect to database
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()

    # process all genres
    for genre in tqdm(genres, file=sys.stdout):

        # check whether this genre is already in DB        
        id = urllib.parse.unquote(genre.split('/')[-1])
        cursor.execute('SELECT id FROM genre WHERE id = ?', (id,))
        result = cursor.fetchall()
        if len(result) > 0:
            continue

        genreinfo = get_genre(genre)
        cursor.execute('''
            INSERT INTO genre (id, name, desc, office, rite)
            VALUES (?, ?, ?, ?, ?)
            ''', (genreinfo['id'], 
                  genreinfo['name'], 
                  genreinfo['desc'],
                  genreinfo['office'],
                  genreinfo['rite'])
        )
    conection.commit()
    conection.close()


def get_feast_all(feasts):
    """Retrieve and store info from all feasts
    """
    # connect to database
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()

    # process all genres
    for feast in tqdm(feasts, file=sys.stdout):

        # check whether this feast is already in DB        
        id = urllib.parse.unquote(feast.split('/')[-1])
        cursor.execute('SELECT id FROM feast WHERE id = ?', (id,))
        result = cursor.fetchall()
        if len(result) > 0:
            continue

        feastinfo = get_feast(feast)
        cursor.execute('''
            INSERT INTO feast (id, name, desc, date)
            VALUES (?, ?, ?, ?)
            ''', (feastinfo['id'], 
                  feastinfo['name'], 
                  feastinfo['desc'],
                  feastinfo['date'])
        )
    conection.commit()
    conection.close()


def main():

    print('--------------------------------------------------------')
    print('RETRIEVING CHANTS INFORMATION')
    print('--------------------------------------------------------')

    create_db_tables()

    print('Genres:')
    genres = cantus_list_genres()
    get_genre_all(genres)

    print('Feasts:')
    feasts = cantus_list_feasts()
    get_feast_all(feasts)
 
    print('Chants:')
    chants = cantus_list_chants()
    get_chant_all(chants)

    return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
    except SystemExit:
        raise
