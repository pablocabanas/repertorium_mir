import numpy as np
import os
import pickle
import sqlite3
import re
from tqdm import tqdm
import gc
import numpy as np
from Bio.Align import PairwiseAligner
from multiprocessing import Pool, cpu_count
from scipy.io import savemat

# constants
STORAGE_FOLDER = './storage/'
CANTUS_DB_FILE = STORAGE_FOLDER + 'chant_info.db'



def format_result(decision, decision_melody, decision_id, decision_char, decision_endchar, decision_cost,
                  seq_folio, seq_y, folios, folder):
    """Format chant detection result for web display.
    """
    numfolios = seq_folio[decision_char[-1]] + 1

    result = []
    for folio in folios:
        src = os.path.join(folder, 'images', 'test', folio + '.png')
        result.append({'src': src,
                       'transcription': [],
                       'melody': [],
                       'id': [],
                       'ymin': [],
                       'ymax': [],
                       'cost': []}
                       )

    for i, beginchar in enumerate(decision_char):
        # begin/end char index for this chant
        # if i == len(decision_char)-1:
        #     endchar = len(seq_folio)
        # else:
        #     endchar = decision_char[i+1]
        endchar = decision_endchar[i]

        # current folio and square
        currentfolio = seq_folio[beginchar]
        y = seq_y[beginchar]
        result[currentfolio]['transcription'].append(decision[i])
        result[currentfolio]['melody'].append(decision_melody[i])
        result[currentfolio]['id'].append(decision_id[i])
        result[currentfolio]['ymin'].append(y)
        result[currentfolio]['cost'].append(decision_cost[i])

        if len(range(beginchar+1, endchar)) == 0:
            result[currentfolio]['ymax'].append(y)
            
        for character in range(beginchar+1, endchar):
            if seq_folio[character] > currentfolio:
                y = seq_y[character-1]
                result[currentfolio]['ymax'].append(y)

                currentfolio = seq_folio[character]
                y = seq_y[character]
                result[currentfolio]['transcription'].append(decision[i])
                result[currentfolio]['melody'].append(decision_melody[i])
                result[currentfolio]['id'].append(decision_id[i])
                result[currentfolio]['ymin'].append(y)
                result[currentfolio]['cost'].append(decision_cost[i])
            
            if character == endchar - 1:
                y = seq_y[character]
                result[currentfolio]['ymax'].append(y)

    return result


def create_js(results, folder, suggested = []):
    """Create javascript file with result.
    """
    jstext= 'const images = ['
    for result in results:
        jstext = jstext + '{src: "' + result['src'] + '", transcription: ['
        for i, _ in enumerate(result['id']):
            link = '<a href="https://cantusindex.org/id/' + result['id'][i] + '">(' + result['id'][i] + ')</a>'
            jstext = jstext + '{cantusid: "' + result['id'][i] + '", '
            jstext = jstext + 'text: "' + result['transcription'][i] + '", '
            jstext = jstext + 'melody: "' + result['melody'][i] + '", '
            jstext = jstext + 'cost: ' + str(result['cost'][i]) + ', '
            jstext = jstext + 'height: ' + str(result['ymin'][i]) + ', '
            jstext = jstext + 'heightmax: ' + str(result['ymax'][i]) + '},'
        jstext = jstext + '],},'
    jstext = jstext + '];'


    jstext = jstext + '\n\n'
    jstext = jstext + 'const suggested = ['
    for sugpage in suggested:
        jstext = jstext + '['
        for cantoid in sugpage:
            jstext = jstext + '"' + cantoid + '", '
        jstext = jstext + '],'
    jstext = jstext + '];'


    with open("./web/appbase.js", "r", encoding="utf-8") as file:
        jscode = file.read()
    with open("./web/indexbase.html", "r", encoding="utf-8") as file:
        htmlcode = file.read()
    
    jscode = jstext + jscode

    with open(os.path.join(folder, 'app.js'), 'w') as file:
        file.write(jscode)
    with open(os.path.join(folder, 'index.html'), 'w') as file:
        file.write(htmlcode)


    

