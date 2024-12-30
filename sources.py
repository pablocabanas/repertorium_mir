# imports
import sys
import requests
import re
import pickle
import os
from urllib.parse import unquote
from bs4 import BeautifulSoup
import sqlite3
from tqdm import tqdm

# url constants
CANTUSDB_URL = 'https://cantusdatabase.org/'
CANTUSDB_LIST_URL = 'https://cantusdatabase.org/sources/?segment=4063'
STORAGE_FOLDER = './storage/'
CANTUSDB_LIST_FILE = STORAGE_FOLDER + 'cantusdbsources.pkl'
CANTUS_DB_FILE = STORAGE_FOLDER + 'chant_info.db'

# session handler
session = requests.Session()
session.headers.update(
    {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0'
    }
)


def cantusdb_list_sources():
    """Retrieve the list of all sources in CantusDB (with image links)
    """
    # load locally stored list
    if os.path.exists(CANTUSDB_LIST_FILE):
        with open(CANTUSDB_LIST_FILE, 'rb') as archivo:
            sources = pickle.load(archivo)
        return sources

    # get first page
    page = session.get(CANTUSDB_LIST_URL)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # number of pages
    div = parsed.find('div', {'class': 'pagination'})
    href = div.find_all('a')[-1].get('href')
    last_page = int(re.search(r'page=(\d+)', href).group(1))

    sources = []

    # iterate over pages
    for p in range(1, last_page + 1):
        print(f'Reading CantusDB page {p}/{last_page}')

        page = session.get(CANTUSDB_LIST_URL + '&page=' + str(p))
        parsed = BeautifulSoup(page.content, 'html.parser')

        # find table and add sources
        table = parsed.find('table').find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            sources.append(
                {
                    'url': cols[2].find('a').get('href'),
                    'siglum': cols[2].find('a').find('b').text,
                    'images': [],
                    'country': cols[0].find('b').text
                }
            )
            aa = cols[5].find_all('a')
            for a in aa:
                sources[-1]['images'].append(a.get('href'))

    # store source sites into files
    with open(CANTUSDB_LIST_FILE, 'wb') as archivo:
        pickle.dump(sources, archivo)
    return sources


def cantusdb_create_tables():
    """Create database tables for CantusDB
    """
    # connect (or create) to database
    conection = sqlite3.connect(CANTUS_DB_FILE)

    cursor = conection.cursor()

    # create source table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS source_cantusdb (
        id TEXT PRIMARY KEY,
        siglum TEXT NOT NULL,
        country TEXT NOT NULL,
        institution TEXT NOT NULL,
        provenance TEXT NOT NULL,
        date TEXT NOT NULL,
        cursus TEXT NOT NULL,
        summary TEXT NOT NULL,
        liturgical TEXT NOT NULL,
        folios TEXT NOT NULL,
        images TEXT NOT NULL,
        iiif TEXT NOT NULL
    )
    ''')

    # create chant table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chant_cantusdb (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        incipit TEXT NOT NULL,
        folio TEXT NOT NULL,
        sequence TEXT NOT NULL,
        feast TEXT NOT NULL,
        genre TEXT NOT NULL,
        position TEXT NOT NULL,
        service TEXT NOT NULL,
        cantusid TEXT NOT NULL,
        mode TEXT NOT NULL,
        differentiae TEXT NOT NULL,
        fulltext_standardized TEXT NOT NULL,
        fulltext_source TEXT NOT NULL,
        volpiano TEXT NOT NULL,
        melody_text TEXT NOT NULL,
        FOREIGN KEY (source) REFERENCES source_cantusdb (id),
        FOREIGN KEY (feast) REFERENCES feast (id),
        FOREIGN KEY (genre) REFERENCES genre (id),
        FOREIGN KEY (cantusid) REFERENCES chant (id)
    )
    ''')

    conection.commit()
    conection.close()


def cantusdb_get_source(source):
    """Retrieve source info from CantusDB
    """
    # get source page
    page = session.get(CANTUSDB_URL + '/' + source['url'])
    parsed = BeautifulSoup(page.content, 'html.parser')

    # get id, siglum, contry ...
    id = unquote(source['url'].split('/')[-1])
    siglum = source['siglum']
    country = source['country']
    institution = ''
    provenance = ''
    date = ''
    cursus = ''
    summary = ''
    liturgical = ''
    folios = ''
    images = ''
    if len(source['images']) > 0:
        for im in source['images']:
            images = images + im + ';'
    iiif = ''

    try:
        dts = parsed.find('dl').find_all('dt')
        dds = parsed.find('dl').find_all('dd')
        for dt, dd in zip(dts, dds):
            if dt.text == 'Holding Institution':
                institution = dd.find('a').text
            elif dt.text == 'Summary':
                summary = dd.text
            elif dt.text == 'Liturgical Occasions':
                liturgical = dd.text
        
        options = parsed.find('select').find_all('option')
        for option in options[1:]:
            folios = folios + option.get('value') + ';'
        
        card = parsed.find_all('div', {'class': 'card-body'})[-1].find('small').text
        match = re.search(r'Provenance:\s*(.*)', card)
        if match:
            provenance = match.group(1).strip()
        match = re.search(r'Date:\s*(.*)', card)
        if match:
            date = match.group(1).strip()
        match = re.search(r'Cursus:\s*(.*)', card)
        if match:
            cursus = match.group(1).strip()
    except:
        print(f'Error parsing source {id}')
        raise
    
    return {
        'id': id,
        'siglum': siglum,
        'country': country,
        'institution': institution,
        'provenance': provenance,
        'date': date,
        'cursus': cursus,
        'summary': summary,
        'liturgical': liturgical,
        'folios': folios,
        'images': images,
        'iiif': iiif
        }


def cantusdb_get_chant(chanturl, sourceid=''):
    """Retrieve chant info from CantusDB
    """
    # get chant page
    page = session.get(CANTUSDB_URL + '/' + chanturl)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # get id, source, incipit ...
    id = unquote(chanturl.split('/')[-1])
    source = sourceid
    incipit = ''
    folio = ''
    sequence = ''
    feast = ''
    genre = ''
    position = ''
    service = ''
    cantusid = ''
    mode = ''
    differentiae = ''
    fulltext_standardized = ''
    fulltext_source = ''
    volpiano = ''
    melody_text = ''

    try:
        incipit = parsed.find('h3').text
        dts = parsed.find('dl').find_all('dt')
        dds = parsed.find('dl').find_all('dd')
        for dt, dd in zip(dts, dds):
            if dt.text == 'Folio':
                folio = dd.text
            elif dt.text == 'Sequence':
                sequence = dd.text
            elif dt.text == 'Feast':
                feast = unquote(dd.find('a').get('href').split('/')[-1])
            elif dt.text == 'Genre':
                genre = unquote(dd.find('a').get('href').split('/')[-1])
            elif dt.text == 'Service':
                service = unquote(dd.find('a').get('href').split('/')[-1])
            elif dt.text == 'Cantus ID':
                cantusid = unquote(dd.find('a').get('href').split('/')[-1])
            elif dt.text == 'Position':
                position = dd.text
            elif dt.text == 'Mode':
                mode = dd.text
            elif dt.text == 'Differentiae Database':
                differentiae = unquote(dd.find('a').get('href').split('/')[-1])
            elif dt.text == 'Full text as in Source (standardized spelling)':
                fulltext_standardized = dd.text
            elif dt.text == 'Full text as in Source (source spelling)':
                fulltext_source = dd.text
            elif dt.text == 'Volpiano':
                volpiano = dd.find('p').text
            elif dt.text == 'Melody with text':
                divs = dd.find_all('div')
                for i in range(0,len(divs),2):
                    melody_text = melody_text + divs[i].text + ';'
                    melody_text = melody_text + divs[i+1].find('pre').text + ';'
    except:
        print(f'Error parsing chant {id}')
        raise

    return {
        'id': id,
        'source': source,
        'incipit': incipit,
        'folio': folio,
        'sequence': sequence,
        'feast': feast,
        'genre': genre,
        'position': position,
        'service': service,
        'cantusid': cantusid,
        'mode': mode,
        'differentiae': differentiae,
        'fulltext_standardized': fulltext_standardized,
        'fulltext_source': fulltext_source,
        'volpiano': volpiano,
        'melody_text': melody_text
    }


def cantusdb_get_chants_source(sourceurl):
    """Retrieve chant list for a given source
    """
    # get list page
    sourceurl = CANTUSDB_URL + sourceurl + '/inventory/'
    page = session.get(sourceurl)
    parsed = BeautifulSoup(page.content, 'html.parser')

    chants = []

    # # number of pages
    # div = parsed.find('div', {'class': 'pagination'})
    # aaa = div.find_all('a')
    # if len(aaa) == 0:
    #     last_page = 1
    # else:
    #     href = aaa[-1].get('href')
    #     last_page = int(re.search(r'page=(\d+)', href).group(1))

    # # iterate over pages
    # for p in range(1, last_page + 1):
    #     page = session.get(sourceurl + '?page=' + str(p))
    #     parsed = BeautifulSoup(page.content, 'html.parser')

    #     # find table and add chants
    #     table = parsed.find('table').find('tbody')
    #     rows = table.find_all('tr')
    #     for row in rows:
    #         cols = row.find_all('td')
    #         chants.append(cols[2].find('a').get('href'))

    table = parsed.find('table').find('tbody')
    rows = table.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        chants.append(cols[8].find('a').get('href'))

    return chants


def cantusdb_get_source_all(sources):
    """Retrieve and store info from all sources/chants in CantusDB
    """
    # connect to database
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()

    print('\nGetting sources:')

    # process all sources
    for source in tqdm(sources, file=sys.stdout):

        # check whether this source is already in DB
        sourceid = unquote(source['url'].split('/')[-1])
        cursor.execute('SELECT id FROM source_cantusdb WHERE id = ?', (sourceid,))
        result = cursor.fetchall()
        if len(result) > 0:
            continue
        
        # retrieve and save source
        try:
            sourceinfo = cantusdb_get_source(source)
        except:
            continue

        cursor.execute('''
            INSERT INTO source_cantusdb
            (id, siglum, country, institution, provenance, date, cursus,
            summary, liturgical, folios, images, iiif)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (sourceinfo['id'],
                  sourceinfo['siglum'], 
                  sourceinfo['country'],
                  sourceinfo['institution'],
                  sourceinfo['provenance'],
                  sourceinfo['date'],
                  sourceinfo['cursus'],
                  sourceinfo['summary'], 
                  sourceinfo['liturgical'],
                  sourceinfo['folios'],
                  sourceinfo['images'],
                  sourceinfo['iiif']
                  ) 
        )

    conection.commit()

    print('\nGetting chants:')

    # process all chants within sources
    for source in tqdm(sources, file=sys.stdout):
        sourceid = unquote(source['url'].split('/')[-1])

        # chants in this source
        try:
            chants = cantusdb_get_chants_source(source['url'])
        except:
            continue

        for chant in chants:
            # check whether this chant is already in DB
            chantid = unquote(chant.split('/')[-1])
            cursor.execute('SELECT id FROM chant_cantusdb WHERE id = ?', (chantid,))
            result = cursor.fetchall()
            if len(result) > 0:
                continue

            # retrieve and save chant
            try:
                chantinfo = cantusdb_get_chant(chant, sourceid=sourceid)
            except:
                continue
            
            cursor.execute('''
                INSERT INTO chant_cantusdb
                (id, source, incipit, folio, sequence, feast, genre,
                position, service, cantusid, mode, differentiae,
                fulltext_standardized, fulltext_source,
                volpiano, melody_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (chantinfo['id'],
                      chantinfo['source'], 
                      chantinfo['incipit'],
                      chantinfo['folio'],
                      chantinfo['sequence'],
                      chantinfo['feast'],
                      chantinfo['genre'],
                      chantinfo['position'], 
                      chantinfo['service'],
                      chantinfo['cantusid'],
                      chantinfo['mode'],
                      chantinfo['differentiae'],
                      chantinfo['fulltext_standardized'],
                      chantinfo['fulltext_source'],
                      chantinfo['volpiano'],
                      chantinfo['melody_text']
                      )
            )
            conection.commit()

    conection.close()


def main():

    print('--------------------------------------------------------')
    print('RETRIEVING SOURCES INFORMATION (CANTUSDB)')
    print('--------------------------------------------------------')

    cantusdb_create_tables()
    sources = cantusdb_list_sources()
    cantusdb_get_source_all(sources)

    return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
    except SystemExit:
        raise
