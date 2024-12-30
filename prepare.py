# imports
import numpy as np
import os
import pickle
import sqlite3
import re
import torch
import matplotlib.pyplot as plt
from tqdm import tqdm
import gc
import numpy as np
import yaml

from srcimages import get_iiif, download_images2


# constants
STORAGE_FOLDER = './storage/'
CANTUS_DB_FILE = STORAGE_FOLDER + 'chant_info.db'


def prepare_cantusdb_for_omr(source, folder, first=0, last=-1, namefirst=None):
    """Prepare cantusdb source for OMR processing
    """

    # get image link for this source
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()
    cursor.execute('SELECT images FROM source_cantusdb WHERE id=?', (source,))
    result = cursor.fetchall()
    if len(result) == 0:
        print(f'ERROR: Source {source} is not in the DB')
        return
    conection.close()
    images = result[0][0].split(';')[0]

    # get iiif
    iiiflink = get_iiif(images)
    if len(iiiflink) == 0:
        print(f'ERROR: Cannot retrieve IIIF')
        return
    
    # create folder/file tree
    sourcefolder = os.path.join(folder, source)
    if os.path.exists(sourcefolder):
        print(f'ERROR: Source already prepared')
        return
    os.makedirs(sourcefolder)
    os.makedirs(os.path.join(sourcefolder, 'images', 'test'))
    os.makedirs(os.path.join(sourcefolder, 'images', 'train'))
    os.makedirs(os.path.join(sourcefolder, 'images', 'validation'))
    yamldata = {
        'names': None,
        'path': sourcefolder,
        'test': 'images/test',
        'train': 'images/train',
        'val': 'images/validation'
    }
    with open(os.path.join(sourcefolder, 'dataset.yaml'), "w") as yaml_file:
        yaml.dump(yamldata, yaml_file, default_flow_style=False)

    # download images
    download_images2(iiiflink, os.path.join(sourcefolder, 'images', 'test'), 
                     first, last, namefirst)




prepare_cantusdb_for_omr('123681',
                         '/media/pablo/DATOS/Trabajo/GitHubToys/repertorium_omr/data',
                         13, 20, '001r')


