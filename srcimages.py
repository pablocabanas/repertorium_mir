# imports
import json
import sys
import requests
import re
import pickle
import os
from urllib.parse import urlparse, parse_qs, urlunparse
from bs4 import BeautifulSoup
import codecs
from PIL import Image
from io import BytesIO
import time
from tqdm import tqdm

# from scipy.fft import ifftn
# from setuptools.command.build_ext import if_dl

# url constants
CANTUS_URL = 'https://cantusindex.org'
CANTUS_LIST_URL = 'https://cantusindex.org/chants'
CANTUS_LIST_FILE = './storage/chants_list.pkl'
CANTUS_INFO_FILE = './storage/chants_info.pkl'
CANTUS_SITES_FILE = './sourcesites.pkl'
CANTUS_SITES_FILETXT = './sourcesites.txt'
MMMO_LIST_URL = 'https://musmed.eu/sources'
MMMO_LIST_FILE = './mmmosources.pkl'
MMMO_LIST_FILE_IIIF = './mmmosources_iiif.pkl'
CANTUSDB_LIST_URL = 'https://cantusdatabase.org/sources/?segment=4063'
CANTUSDB_LIST_FILE = './cantusdbsources.pkl'
CANTUSDB_LIST_FILE_IIIF = './cantusdbsources_iiif.pkl'
DB_FOLDER = '/mnt/share/pcabanas/BBDD/Medieval/'

# session handler
session = requests.Session()
session.headers.update(
    {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0'
    }
)


def cantus_list_chants():
    """Retrieve the list of all chants in Cantus Index
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

    with open(CANTUS_LIST_FILE, 'wb') as archivo:
        pickle.dump(chants, archivo)

    return chants


def cantus_chant_source_sites(chant_url):
    """Retrieve the source sites for a given chant
    """
    page = session.get(CANTUS_URL + chant_url)
    parsed = BeautifulSoup(page.content, 'html.parser')

    site_urls = []
    site_names = []

    try:
        # get ajax link
        ajaxlink = parsed.find('a', {'id': 'ajax-link'}).get('href')

        # get json data
        page = session.get(CANTUS_URL + ajaxlink)
        jsondata = json.loads(page.text[2:])[1]['data']
        parsed = BeautifulSoup(jsondata, 'html.parser')

        # parse info to get source sites
        # if parsed.text.startswith('No published source'):
        #    return site_urls, site_names
        rows = parsed.find('table').find_all('tr')
        for row in rows:
            col = row.find('td')
            site_names.append(col.find('b').text)
            site_urls.append(col.find('br').next_sibling.strip())

        return site_urls, site_names

    except AttributeError:
        return site_urls, site_names


def cantus_source_sites(chants):
    """Retrieve the list of all source sites in Cantus Index
    """
    # load locally stored dict
    if os.path.exists(CANTUS_SITES_FILE):
        with open(CANTUS_SITES_FILE, 'rb') as archivo:
            sites = pickle.load(archivo)
        return sites

    sites = {}

    print('Retrieving source sites from chants:')
    for chant in chants:
        print(' ' + chant)
        site_url, site_names = cantus_chant_source_sites(chant)
        for url, name in zip(site_url, site_names):
            sites[url] = name

    # store source sites into files
    with open(CANTUS_SITES_FILE, 'wb') as archivo:
        pickle.dump(sites, archivo)
    with open(CANTUS_SITES_FILETXT, 'w') as archivo:
        for clave in sites.keys():
            archivo.write(f'{clave} ({sites[clave]})\n')

    return sites


def cantus_source_sites2():
    """Retrieve the list of all source sites in Cantus Index
    """
    # load locally stored dict
    if os.path.exists(CANTUS_SITES_FILE):
        with open(CANTUS_SITES_FILE, 'rb') as archivo:
            sites = pickle.load(archivo)
        return sites

    page = session.get(CANTUS_URL)
    parsed = BeautifulSoup(page.content, 'html.parser')
    aa = parsed.find('ul', {'class': ''}).find_all('a')

    sites = {}
    for a in aa:
        url = a.get('href')
        if url not in sites:
            sites[url] = a.text

    # store source sites into files
    with open(CANTUS_SITES_FILE, 'wb') as archivo:
        pickle.dump(sites, archivo)
    with open(CANTUS_SITES_FILETXT, 'w') as archivo:
        for clave in sites.keys():
            archivo.write(f'{clave} ({sites[clave]})\n')

    return sites


def get_chant(chant):
    """Retrieve chant info
    """
    # get chant page
    page = session.get(CANTUS_URL + '/' + chant)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # chant text
    fulltext = parsed.find('div', {'class': 'field-item even'}).text

    # chant occurences
    try:
        ajaxlink = CANTUS_URL + parsed.find('a', {'id': 'ajax-link'}).get('href')
        jsonres = session.get(ajaxlink).text
        jsonres = jsonres.replace('\\/', '/')
        jsonres = jsonres.encode('utf-8').decode('unicode_escape')
        jsonres = json.loads(jsonres[6:-1])
        jsonhtml = BeautifulSoup(jsonres[1]['data'], 'html.parser')
        jsontables = jsonhtml.find_all('table',
                                       {'class': 'table table-hover table-striped sticky-enabled'})
        concordances = len(jsontables[1].find('tbody').find_all('tr'))
    except Exception:
        print(f'Chant {chant}: concordances not found')
        concordances = 0

    # melodies
    try:
        ajaxlink2 = CANTUS_URL + parsed.find('a', {'id': 'ajax-link--2'}).get('href')
        jsonres2 = session.get(ajaxlink2).text
        jsonres2 = jsonres2.replace('\\/', '/')
        jsonres2 = jsonres2.encode('utf-8').decode('unicode_escape')
        jsonres2 = json.loads(jsonres2[6:-1])
        jsonhtml2 = BeautifulSoup(jsonres2[1]['data'], 'html.parser')
        melodies = len(jsonhtml2.find('table').find('tbody').find_all('tr'))
    except Exception:
        print(f'Chant {chant}: melodies not found')
        melodies = 0

    return {
        'url': chant,
        'text': fulltext,
        'concordances': concordances,
        'melodies': melodies
        }


def get_chant_all(chants):
    """Retrieve chant info from all chants
    """
    # load locally stored info
    if os.path.exists(CANTUS_INFO_FILE):
        print('Retrieving chants info from file ...')
        with open(CANTUS_INFO_FILE, 'rb') as archivo:
            chantinfo = pickle.load(archivo)
        return chantinfo

    chantinfo = []
    print('Retrieving chants info from cantusindex ...')
    for chant in tqdm(chants):
        chantinfo.append(get_chant(chant))
    # store chants info into file
    with open(CANTUS_INFO_FILE, 'wb') as archivo:
        pickle.dump(chantinfo, archivo)
    return chantinfo


def mmmo_list_sources():
    """Retrieve the list of all sources in MMO (with image links)
    """
    # load locally stored list
    if os.path.exists(MMMO_LIST_FILE):
        with open(MMMO_LIST_FILE, 'rb') as archivo:
            sources = pickle.load(archivo)
        return sources

    # get first page
    page = session.get(MMMO_LIST_URL)
    parsed = BeautifulSoup(page.content, 'html.parser')

    # number of pages
    href = parsed.find('a', {'title': 'Go to last page'}).get('href')
    last_page = int(re.search(r'page=(\d+)', href).group(1))

    sources = []

    # iterate over pages
    for p in range(0, last_page + 1):
        print(f"Reading MMMO page {p}/{last_page}")

        page = session.get(MMMO_LIST_URL + '?page=' + str(p))
        parsed = BeautifulSoup(page.content, 'html.parser')

        # find table and add sources
        table = parsed.find('table', class_='views-table cols-6').find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            sources.append(
                {
                    'url': cols[0].find('a').get('href'),
                    'siglum': cols[0].find('a').text,
                    'images': []
                }
            )
            aa = cols[5].find_all('a')
            for a in aa:
                sources[-1]['images'].append(a.get('href'))

    # store source sites into files
    with open(MMMO_LIST_FILE, 'wb') as archivo:
        pickle.dump(sources, archivo)
    return sources


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
        print(f"Reading CantusDB page {p}/{last_page}")

        page = session.get(CANTUSDB_LIST_URL + '&page=' + str(p))
        parsed = BeautifulSoup(page.content, 'html.parser')

        # find table and add sources
        table = parsed.find('table',
                            class_='table table-sm small table-bordered table-responsive').find('tbody')
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            sources.append(
                {
                    'url': cols[0].find('a').get('href'),
                    'siglum': cols[1].find('b').text,
                    'images': []
                }
            )
            aa = cols[4].find_all('a')
            for a in aa:
                sources[-1]['images'].append(a.get('href'))

    # store source sites into files
    with open(CANTUSDB_LIST_FILE, 'wb') as archivo:
        pickle.dump(sources, archivo)
    return sources


def image_list_sites(sources):
    """Retrieve the list of all unique image sites
    """
    sites = {}
    for i, source in enumerate(sources):
        for imagelink in source['images']:
            parurl = urlparse(imagelink)
            # clave = f'{parurl.scheme}://{parurl.netloc}'
            clave = parurl.netloc
            if clave not in sites.keys():
                sites[clave] = []
            sites[clave].append(i)
    return sites


def get_iiif(imagelink):
    """Retrieve the IIIF from an image repository link.
    If the IIIF is not available, return a list of image links.
    """
    # parse url
    parurl = urlparse(imagelink)

    # determine manifest url (or image links)
    iiiflink = ''

    try:
        if parurl.netloc == 'unipub.uni-graz.at':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            title = parsed.find('li', {'id': 'tab-content-titleinfo'}).find('a')
            if title is not None:
                page = session.get('https://unipub.uni-graz.at' + title.get('href'))
                parsed = BeautifulSoup(page.content, 'html.parser')
            href = parsed.find('a', {'target': 'iiif-manifest'}).get('href')
            iiiflink = 'https://unipub.uni-graz.at' + href

        elif parurl.netloc == 'manuscripta.at':
            if parurl.path == '/hs_detail.php':
                page = session.get(imagelink)
                parsed = BeautifulSoup(page.content, 'html.parser')
                pagedesc = parsed.find('div', {'id': 'content1'}).find('iframe').get('src')
                page = session.get(pagedesc)
                parsed = BeautifulSoup(page.content, 'html.parser')
                res = parsed.find('span', {'id': 'ms_code'}).text
            else:
                res = str(parurl.path).split('/')[2]
            iiiflink = 'https://manuscripta.at/diglit/iiif/' + res + '/manifest.json'

        elif parurl.netloc == 'digi.landesbibliothek.at':
            res = str(parurl.path).split('/')[4]
            iiiflink = 'https://digi.landesbibliothek.at/viewer/api/v1/records/' + res + '/manifest/'

        elif parurl.netloc == 'www.cantusplanus.at':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            mirador = parsed.find('body').find('script', {'src': ''})
            iiiflink = re.search(r"manifestUri:\s*'([^']*)'", mirador.text).group(1)

        elif parurl.netloc == 'digital.onb.ac.at' or parurl.netloc == 'data.onb.ac.at':
            iiiflink = []
            if parurl.path.split('/')[1] != 'dtl':
                page = session.get(imagelink)
                docid = parse_qs(urlparse(page.url).query)['docid'][0]
                pagesearch = 'https://search.onb.ac.at/primo_library/libweb/webservices/rest/primo-explore/v1/pnxs/L/'
                pagesearch = (pagesearch + docid +
                              '?vid=ONB&lang=de_DE&search_scope=ONB_gesamtbestand&adaptor=Local Search Engine')
                jwttoken = ('Bearer eyJraWQiOiJwcmltb0V4cGxvcmVQcml2YXRlS2V5LU9OQiIsImFsZyI6IkVTMjU2In0.' +
                            'eyJpc3MiOiJQcmltbyIsImp0aSI6IiIsImNtanRpIjpudWxsLCJleHAiOjE3MjkwMTMyNjEsIml' +
                            'hdCI6MTcyODkyNjg2MSwidXNlciI6ImFub255bW91cy0xMDE0XzE3Mjc0MSIsInVzZXJOYW1lIj' +
                            'pudWxsLCJ1c2VyR3JvdXAiOiJHVUVTVCIsImJvckdyb3VwSWQiOm51bGwsInViaWQiOm51bGwsI' +
                            'mluc3RpdHV0aW9uIjoiT05CIiwidmlld0luc3RpdHV0aW9uQ29kZSI6Ik9OQiIsImlwIjoiNzku' +
                            'MTE3LjE2Ny4xNTciLCJwZHNSZW1vdGVJbnN0IjpudWxsLCJvbkNhbXB1cyI6ImZhbHNlIiwibGF' +
                            'uZ3VhZ2UiOiJkZV9ERSIsImF1dGhlbnRpY2F0aW9uUHJvZmlsZSI6IiIsInZpZXdJZCI6Ik9OQi' +
                            'IsImlsc0FwaUlkIjpudWxsLCJzYW1sU2Vzc2lvbkluZGV4IjoiIiwiand0QWx0ZXJuYXRpdmVCZ' +
                            'WFjb25JbnN0aXR1dGlvbkNvZGUiOiJPTkIifQ.rVn9pBcEGNrrTWZXqdiJpXkBfDSt5LAzLT-kq' +
                            'oD86jlwC9G_1VkMfb3ZTeLXWBwZaxZpxjFM8TVG6LRRfqCNSw')
                page = session.get(pagesearch, headers={'Accept': 'application/json',
                                                        'Authorization': jwttoken})
                jsonres = json.loads(page.text)
                for link in jsonres['delivery']['link']:
                    if link['displayLabel'] == 'Digitales Objekt':
                        imagelink = link['linkURL']
                        break

            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            res = parsed.find('input', {'id': 'formViewer:doc'}).get('value')
            lastnum = parsed.find_all('span',
                                      {'class': 'default-thumb lazySpanThumb'})[-1].get('ordernum')
            jsonlink = 'https://digital.onb.ac.at/RepViewer/service/viewer/imageData?doc=' \
                       + res + '&from=1&to=' + lastnum
            jsonentries = json.loads(session.get(jsonlink).text)['imageData']
            for jsonentry in jsonentries:
                iiiflink.append('https://digital.onb.ac.at/RepViewer/image?'
                                + jsonentry['queryArgs'] + '&s=1.0')

        elif parurl.netloc == 'digital.library.sydney.edu.au':
            iiiflink = []
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            imgs = parsed.find('div', {'id': 'miniMe'}).find_all('img')
            for img in imgs:
                lazy = urlparse(img.get('lazy'))
                iiiflink.append(parurl.scheme + '://'
                                + parurl.netloc
                                + str(lazy.path).split('-')[0]
                                + '-max?'
                                + lazy.query)

        elif parurl.netloc == 'lib.ugent.be':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            iiiflink = parsed.find('ul', {'class': 'list-unstyled'}).find('a').get('href')

        elif parurl.netloc == 'cantus.simssa.ca':
            page = session.get(imagelink, headers={'Accept': 'application/json'})
            jsonres = json.loads(page.text)
            iiiflink = jsonres['manifest_url']

        elif parurl.netloc == 'fragmentarium.ms':
            if parurl.path.startswith('/view'):
                res = parurl.path.split('/')[3]
            else:
                res = parurl.path.split('/')[2]
            iiiflink = 'https://fragmentarium.ms/metadata/iiif/' + str(res) + '/manifest.json'

        elif parurl.netloc == 'fishercollections.library.utoronto.ca':
            pid = parurl.path.split('/')[-1]
            iiiflink = 'https://iiif.library.utoronto.ca/presentation/v2/' + pid + '/manifest'

        elif parurl.netloc == 'www.e-codices.unifr.ch':
            page = session.get(imagelink)
            iiiflink = re.search(r"http[^ ]*iiif[^ ]*manifest\.json", page.text).group(0)
            iiiflink = iiiflink.replace('\\/', '/')

        elif parurl.netloc == 'www.manuscriptorium.com':
            iiiflink = []
            page = session.get(imagelink)
            pid = parse_qs(parurl.query).get('pid')[0]
            ajaxdata = {
                'parameters[id]': session.cookies.values()[-1],
                'parameters[request]': 'show_record_id',
                'parameters[value]': pid,
                'parameters[fttQuery]': '',
                'parameters[recID]': pid
            }
            response = session.post('https://www.manuscriptorium.com/apps/manu3_srn_async.php',
                                    data=ajaxdata)
            jsonres = json.loads(response.text)
            parsed = BeautifulSoup(jsonres['response']['result'], 'html.parser')
            imgs = json.loads(parsed.find('div', {'id': 'lorisList'}).text)['imageList']
            for img in imgs:
                iiiflink.append('https://imagines.manuscriptorium.com/loris/'
                                + pid
                                + '/' + img['id']
                                + '/full/full/0/default.jpg')

        elif parurl.netloc == 'digital.staatsbibliothek-berlin.de':
            ppn = parse_qs(parurl.query).get('PPN')[0]
            iiiflink = 'https://content.staatsbibliothek-berlin.de/dc/' + ppn + '/manifest'

        elif parurl.netloc == 'digital.bib-bvb.de':
            iiiflink = []
            pid = parse_qs(parurl.query).get('pid')[0]
            page = session.get(
                'http://digital.bib-bvb.de/webclient/DeliveryManager?custom_att_2=simple_viewer&pid='
                + pid)
            jsplink = re.search(r'getStructMap\.jsp[^"]*(?=")', page.text).group(0)
            page = session.get(
                'http://digital.bib-bvb.de/view/bvb_mets/'
                + jsplink)
            jsonentries = json.loads(page.text)[0]['children']
            pids = []

            def find_pid(children):
                for child in children:
                    if 'li_attr' in child:
                        pids.append(child['li_attr']['pid'])
                    elif 'children' in child:
                        find_pid(child['children'])

            find_pid(jsonentries)
            for pid in pids:
                page = session.get('http://digital.bib-bvb.de/webclient/DeliveryManager?'
                                   + 'custom_att_3=fwd_ref&pds_handle=GUEST&frameId=1&hideLogo=true&'
                                   + 'pid=' + pid)
                iiiflink.append('http://digital.bib-bvb.de/webclient/StreamGate?'
                                + urlparse(page.url).query)

        elif parurl.netloc == 'fuldig.hs-fulda.de':
            ppn = re.search(r'/viewer/image/([^/]+)/', parurl.path).group(1)
            iiiflink = 'https://fuldig.hs-fulda.de/viewer/api/v1/records/' + ppn + '/manifest/'

        elif parurl.netloc == 'digital.blb-karlsruhe.de':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            pid = parsed.find('var', {'id': 'publicationID'}).get('value')
            iiiflink = 'https://digital.blb-karlsruhe.de/i3f/v20/' + pid + '/manifest'

        elif parurl.netloc == 'digital.dombibliothek-koeln.de':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            href = parsed.find('li', {'id': 'tab-content-titleinfo'}).find('a').get('href')
            pid = href.rstrip('/').split('/')[-1]
            iiiflink = 'https://digital.dombibliothek-koeln.de/hs/i3f/v20/' + pid + '/manifest'

        elif parurl.netloc == 'www.digitale-sammlungen.de':
            pid = parurl.path.rstrip('/').split('/')[-1]
            iiiflink = 'https://api.digitale-sammlungen.de/iiif/presentation/v2/' + pid + '/manifest'

        elif parurl.netloc == 'diglib.hab.de':
            iiiflink = []
            pid = '/'.join(parurl.path.rstrip('/').split('/')[:-1])
            pid = pid[1:]
            page = session.get('https://diglib.hab.de/show_image.php?dir=' + pid)
            parsed = BeautifulSoup(page.content, 'html.parser')
            imgs = parsed.find('form', {'name': 'selectimage'}).find_all('option')[1:]
            for img in imgs:
                page = session.get('https://diglib.hab.de/show_image.php?dir='
                                   + pid + '&pointer=' + img.get('value'))
                parsed = BeautifulSoup(page.content, 'html.parser')
                iiiflink.append(parsed.find('div', {'class': 'display'}).find('img').get('src'))

        elif parurl.netloc == 'hlbrm.digitale-sammlungen.hebis.de':
            iiiflink = []
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            zoom = parsed.find('var', {'id': 'webcacheSizes'}).get('value').split(',')[-1]
            imgs = parsed.find('select', {'id': 'goToPage'}).find_all('option')
            for img in imgs:
                iiiflink.append('https://hlbrm.digitale-sammlungen.hebis.de/download/webcache/'
                                + zoom + '/' + img.get('value'))

        elif parurl.netloc == 'cantus.app.uni-regensburg.de':
            iiiflink = []
            page = session.get(imagelink + '/ThumbnailFrame.html')
            parsed = BeautifulSoup(page.content, 'html.parser')
            imgs = parsed.find_all('a')
            for img in imgs:
                iiiflink.append(imagelink + '/images/' + img.text + '.jpg')

        elif parurl.netloc == 'cecilia.mediatheques.grand-albigeois.fr':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            href = parsed.find('div',
                               {'class': 'page_content media_image'}).find('a').get('href')
            page = session.get('https://' + parurl.netloc + href)
            iiiflink = ('https://' + parurl.netloc + '/'
                        + re.search(r"api/viewer/lgiiif\?url[^']*(?=')", page.text).group(0))

        elif parurl.netloc == 'bvmm.irht.cnrs.fr':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            iiiflink = parsed.find('div', {'id': 'miradorViewer'}).get('data-manifest')

        elif parurl.netloc == 'gallica.bnf.fr':
            pid = re.search(r"ark:/[^/]+/[^/]*", parurl.path).group(0)
            iiiflink = 'https://gallica.bnf.fr/iiif/' + pid + '/manifest.json'

        elif parurl.netloc == 'ege.denison.edu':
            iiiflink = []
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            places = parsed.find('div', {'id': 'egeCollection'}).find_all('a')
            for place in places:
                page = session.get('http://ege.denison.edu/' + place.get('href'))
                parsed = BeautifulSoup(page.content, 'html.parser')
                imgs = parsed.find_all('a', {'class': 'highslide'})
                for img in imgs:
                    iiiflink.append('http://ege.denison.edu/' + img.get('href'))

        elif parurl.netloc == 'urn.fi':
            iiiflink = []
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            netloc = urlparse(page.url).netloc
            imgs = parsed.find_all('a', string='Download')
            for img in imgs[1:]:
                iiiflink.append('https://' + netloc + '/' + img.get('href'))

        elif parurl.netloc == 'digital.bodleian.ox.ac.uk':
            page = session.get(imagelink)
            pid = urlparse(page.url).path.rstrip('/').split('/')[-1]
            iiifmanifest = ('https://iiif.bodleian.ox.ac.uk/iiif/manifest/'
                            + pid + '.json')
            iiiflink = parse_iiif_links(iiifmanifest)
            for idx, imlink in enumerate(iiiflink):
                iiiflink[idx] = iiiflink[idx] + '/full/full/0/default.jpg'

        elif parurl.netloc == 'iiif.bodleian.ox.ac.uk':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            iiifmanifest = parsed.find('div', {'class': 'uv'}).get('data-uri')
            iiiflink = parse_iiif_links(iiifmanifest)
            for idx, imlink in enumerate(iiiflink):
                iiiflink[idx] = iiiflink[idx] + '/full/full/0/default.jpg'

        elif parurl.netloc == 'www.internetculturale.it':
            iiiflink = []
            pid = parse_qs(parurl.query).get('id')[0]
            teca = parse_qs(parurl.query).get('teca')[0]
            page = session.get('https://www.internetculturale.it/jmms/magparser?id='
                               + pid + '&teca=' + teca
                               + '&mode=all&fulltext=0')
            parsed = BeautifulSoup(page.content, 'lxml-xml')
            imgs = parsed.find_all('page')
            for img in imgs:
                iiiflink.append('https://www.internetculturale.it/jmms/' + img.get('src'))

        elif parurl.netloc == 'daten.digitale-sammlungen.de':
            pid = parurl.path.rstrip('/').split('/')[1]
            iiiflink = 'https://api.digitale-sammlungen.de/iiif/presentation/v2/' + pid + '/manifest'

        elif parurl.netloc == 'objects.library.uu.nl':
            pid = parse_qs(parurl.query).get('obj')[0]
            iiiflink = 'https://objects.library.uu.nl/manifest/iiif/v3/' + pid

        elif parurl.netloc == 'ndhadeliver.natlib.govt.nz':
            pid = parse_qs(parurl.query).get('dps_pid')[0]
            iiiflink = ('https://ndhadeliver.natlib.govt.nz/delivery/iiif/presentation/3/'
                        + pid + '/manifest')

        elif parurl.netloc == 'www.bibliotekacyfrowa.pl':
            page = session.get(imagelink)
            mainlink = re.search(r'"mainLink":"([^"]+)"', page.text).group(1)
            mainlink = codecs.decode(mainlink, 'unicode_escape')
            page = session.get(mainlink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            iiiflink = 'https:' + parsed.find('a', {'id': 'lnkManifestIIIF'}).get('href')

        elif parurl.netloc == 'dlc.library.columbia.edu':
            page = session.get(imagelink)
            parsed = BeautifulSoup(page.content, 'html.parser')
            iiiflink = parsed.find('div', {'id': 'mirador'}).get('data-manifest')
            manifest = json.loads(session.get(iiiflink).text)
            iiiflink = []
            for sequence in manifest.get('items', []):
                for canvas in sequence.get('items', []):
                    for image in canvas.get('items', []):
                        resource = image.get('body')
                        service = resource.get('service')[0]
                        if '@id' in service:
                            iiiflink.append(service['@id'] + '/full/full/0/default.jpg')

        elif parurl.netloc == 'digi.vatlib.it':
            iiiflink = []
            pid = parurl.path.rstrip('/').split('/')[2]
            iiifmanifest = 'https://digi.vatlib.it/iiif/' + pid + '/manifest.json'
            imlinks = parse_iiif_links(iiifmanifest)
            for idx, imlink in enumerate(imlinks):
                jsondata = json.loads(session.get(imlink).content)
                pid1, pid2 = jsondata['service']['@id'].split('/')[-2:]
                iiiflink.append('https://digi.vatlib.it/pub/digit/' + pid1
                                + '/iiif/' + pid2
                                + '/full/full/0/native.jpg')

        elif parurl.netloc == 'handle.slv.vic.gov.au':
            iiiflink = []
            page = session.get(imagelink)
            jsonlink = ('https://viewerapi.slv.vic.gov.au/?' +
                        urlparse(page.url).query +
                        '&dc_arrays=1')
            jsonres = json.loads(session.get(jsonlink).text)
            claves = list(jsonres['summary']['jq_tree'].keys())
            if jsonres['summary']['jq_tree'][claves[0]][0]['name'] == 'Table of Contents':
                clave = claves[0]
            else:
                clave = claves[1]
            for img in jsonres['summary']['file'][clave]:
                ref = re.search(r'\["file"\]\["(.*?)"\]', img['$ref']).group(1)
                iiiflink.append('https://rosetta.slv.vic.gov.au/delivery/DeliveryManagerServlet?' +
                                'dps_func=stream&dps_pid=' +
                                ref)
                
        else:
            print('   Site ' + parurl.netloc + ' unknown')

    except Exception as e:
        print(f'   When retrieving {imagelink}, found this error: {e}')
        iiiflink = ''

    return iiiflink


def parse_iiif_links(iiiflink):
    """Extract image links from a IIIF manifest link.
    """
    imagelist = []
    manifest = json.loads(session.get(iiiflink).text)
    for sequence in manifest.get('sequences', []):
        for canvas in sequence.get('canvases', []):
            for image in canvas.get('images', []):
                resource = image.get('resource', {})
                if '@id' in resource:
                    imagelist.append(resource['@id'])
    return imagelist


def fix_iiif_link(iiiflink):
    """Fix incomplete iiif link.
    """
    parsed_url = urlparse(iiiflink)
    path_parts = parsed_url.path.strip('/').split('/')
    if len(path_parts) < 5:
        return iiiflink
    path_parts[3] = 'full'
    path_parts[4] = 'full'
    path_parts[5] = '0'
    new_path = '/' + '/'.join(path_parts)
    iiiflink = urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        new_path,
        parsed_url.params,
        parsed_url.query,
        parsed_url.fragment
    ))
    return iiiflink


def download_images(iiiflink, folder):
    """Download images from a list of image links or a IIIF manifest link.
    """
    max_pixels = 1_000_000
    imagelist = []

    # get and parse iiif manifest
    if isinstance(iiiflink, str):
        imagelist = parse_iiif_links(iiiflink)
    elif isinstance(iiiflink, list):
        imagelist = iiiflink

    if not os.path.exists(folder):
        os.makedirs(folder)

    # download image list
    for idx, imagelink in enumerate(imagelist):
        file_path = os.path.join(folder, f"{idx}.tif")
        if os.path.exists(file_path):
            continue

        print(f'  ({idx}/{len(imagelist)-1}) Downloading {imagelink}')
        response = session.get(imagelink)
        if response.status_code != 200:
            continue
        try:
            imagen = Image.open(BytesIO(response.content))
        except Exception as e:
            raise e

        if imagen.size[0] * imagen.size[1] > max_pixels:
            scale = (max_pixels / (imagen.size[0] * imagen.size[1])) ** 0.5
            new_size = (int(imagen.size[0] * scale), int(imagen.size[1] * scale))
            imagen = imagen.resize(new_size, Image.Resampling.LANCZOS)

        # save in TIFF with LZW compression
        imagen.save(file_path, format='TIFF', compression='tiff_lzw')

        # wait a little
        time.sleep(2)


# def generate_page_names(npages, name):
#     """Generate page names
#     """
#     pages = []
#     is_numeric = name.isdigit()
    
#     if is_numeric:
#         # Generar nombres para nombres puramente numéricos
#         start = int(name)
#         pages = [str(start + i).zfill(len(name)) for i in range(npages)]
#     else:
#         # Manejo para nombres con "r" y "v" al final
#         prefix = ''.join(filter(str.isdigit, name))  # Extraer la parte numérica
#         suffix = name[len(prefix):]  # Extraer el resto (como "r" o "v")
        
#         if suffix in ["r", "v"]:
#             start = int(prefix)
#             for i in range(npages):
#                 current_page = start + i
#                 pages.append(f"{str(current_page).zfill(len(prefix))}r")
#                 pages.append(f"{str(current_page).zfill(len(prefix))}v")
#             pages = pages[:npages]
#         else:
#             raise ValueError(f"Unrecognized page format: {name}")

#     return pages


def generate_page_names(npages, name):
    
    # Extraer la parte numérica y el sufijo (si existe)
    match = re.match(r"(\d+)([rv]?)", name)
    if not match:
        raise ValueError("El formato del nombre de página no es válido.")
    
    number, suffix = match.groups()
    number = int(number)  # Convertir la parte numérica a entero
    
    # Lista para almacenar los nombres de las páginas generadas
    pages = []
    
    # Determinar si el sufijo debe manejarse
    if suffix:  # Si hay sufijo (como 'r' o 'v')
        suffix_order = ["r", "v"]  # Orden de alternancia entre recto y verso
        current_suffix_index = suffix_order.index(suffix)  # Posición inicial
        
        for _ in range(npages):
            pages.append(f"{number:03d}{suffix_order[current_suffix_index]}")  # Generar página con sufijo
            current_suffix_index = (current_suffix_index + 1) % 2  # Alternar entre 'r' y 'v'
            if current_suffix_index == 0:  # Cambiamos de página solo después de 'v'
                number += 1
    else:  # Si no hay sufijo, generar solo números consecutivos
        for _ in range(npages):
            pages.append(str(number))  # Agregar el número directamente
            number += 1  # Incrementar el número de página
    
    return pages


def download_images2(iiiflink, folder, first=0, last=-1, namefirst=None, format='png'):
    """Download images from a list of image links or a IIIF manifest link.
    """
    max_pixels = 1_000_000
    imagelist = []

    # get and parse iiif manifest
    if isinstance(iiiflink, str):
        imagelist = parse_iiif_links(iiiflink)
    elif isinstance(iiiflink, list):
        imagelist = iiiflink

    # select images
    if last>0:
        last = last+1
    imagelist = imagelist[first:last]

    # generate page names
    if namefirst is None:
        namefirst = '0'
    pages = generate_page_names(len(imagelist), namefirst)

    ext = 'tif'
    if format == "png":
        ext = 'png'

    if not os.path.exists(folder):
        os.makedirs(folder)

    # download image list
    for idx, imagelink in enumerate(imagelist):
        file_path = os.path.join(folder, f"{pages[idx]}.{ext}")
        if os.path.exists(file_path):
            continue

        print(f'  ({idx}/{len(imagelist)-1}) Downloading {imagelink}')
        response = session.get(imagelink)
        if response.status_code != 200:
            continue
        try:
            imagen = Image.open(BytesIO(response.content))
        except Exception as e:
            raise e

        if imagen.size[0] * imagen.size[1] > max_pixels:
            scale = (max_pixels / (imagen.size[0] * imagen.size[1])) ** 0.5
            new_size = (int(imagen.size[0] * scale), int(imagen.size[1] * scale))
            imagen = imagen.resize(new_size, Image.Resampling.LANCZOS)

        # save in PNG or TIFF with LZW compression
        if format=="png":
            imagen.save(file_path, format='PNG')
        else:
            imagen.save(file_path, format='TIFF', compression='tiff_lzw')

        # wait a little
        time.sleep(2)


def get_iiif_all(sources):
    """Retrieve IIIF link or image links for all sources in a list.
    """
    stats = {
        'success': [],
        'missing': [],
        'fail': []
    }

    for idx, source in enumerate(sources):
        print(f"({idx}/{len(sources)-1}) Getting links for {source['url']}")
        if len(source['images']) == 0:
            sources[idx]['iiiflink'] = ''
            stats['missing'].append(idx)
        else:
            iiiflink = ''
            for imagelink in source['images']:
                iiiflink = get_iiif(imagelink)
                if len(iiiflink) != 0:
                    sources[idx]['iiiflink'] = iiiflink
                    stats['success'].append(idx)
                    break
            if len(iiiflink) == 0:
                sources[idx]['iiiflink'] = ''
                stats['fail'].append(idx)

    n_succes = len(stats['success'])
    n_missing = len(stats['missing'])
    n_fail = len(stats['fail'])
    print(f'Success: {n_succes}/{len(sources)}')
    print(f'Fail: {n_fail}/{len(sources)}')
    print(f'Missing: {n_missing}/{len(sources)}')

    return sources


def download_images_all(sources, folder):
    """Download images from a list of sources.
    """
    if not os.path.exists(folder):
        os.makedirs(folder)

    for idx, source in enumerate(sources):
        print(f"({idx}/{len(sources)-1}) Downloading images for {source['url']}")

        subfolder = source['siglum'] + '_' + source['url'].split('/')[-1]
        subfolder = re.sub(r'[^\w\s-]', '_', subfolder).strip()
        subfolder = os.path.join(folder, subfolder)

        if len(source['iiiflink']) == 0:
            print(f'  ERROR: image links not found')
            continue

        try:
            download_images(source['iiiflink'], subfolder)
        except Exception as e:
            # update iiiflink
            print(f'  Exception: {e}')
            time.sleep(10)
            print(f'  WARNING: updating source iiiflink')
            source['iiiflink'] = get_iiif_all([source])[0]['iiiflink']
            try:
                download_images(source['iiiflink'], subfolder)
            except Exception as ee:
                continue


def main():
    chants = cantus_list_chants()
    chantinfo = get_chant_all(chants[0:99])
    # sites = cantus_source_sites(chants)
    sites = cantus_source_sites2()

    # print('--------------------------------------------------------')
    # print('RETRIEVING CANTUSDB SOURCES')
    # print('--------------------------------------------------------')
    # if os.path.exists(CANTUSDB_LIST_FILE_IIIF):
    #     with open(CANTUSDB_LIST_FILE_IIIF, 'rb') as archivo:
    #         sources = pickle.load(archivo)
    # else:
    #     sources = cantusdb_list_sources()
    #     # imagesites = image_list_sites(sources)
    #     sources = get_iiif_all(sources)
    #     with open(CANTUSDB_LIST_FILE_IIIF, 'wb') as archivo:
    #         pickle.dump(sources, archivo)
    # folder = os.path.join(DB_FOLDER, 'CANTUSDB')
    # while True:
    #     download_images_all(sources, folder)
    #     time.sleep(60)

    print('--------------------------------------------------------')
    print('RETRIEVING MMMO SOURCES')
    print('--------------------------------------------------------')
    if os.path.exists(MMMO_LIST_FILE_IIIF):
        with open(MMMO_LIST_FILE_IIIF, 'rb') as archivo:
            sources = pickle.load(archivo)
    else:
        sources = mmmo_list_sources()
        imagesites = image_list_sites(sources)
        sources = get_iiif_all(sources)
        with open(MMMO_LIST_FILE_IIIF, 'wb') as archivo:
            pickle.dump(sources, archivo)
    folder = os.path.join(DB_FOLDER, 'MMMO')

    # iiiflink = get_iiif(sources[142]['images'][0])
    # print('Downloading ' + sources[140]['siglum'])
    # iiiflink = get_iiif(sources2[1]['images'][0])
    # download_images(iiiflink, os.path.join(DB_FOLDER, 'CANTUSDB', sources2[1]['siglum']))

    return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
    except SystemExit:
        raise
