# imports
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
import webbrowser
from web import format_result, create_js
from database import find_chant_suggested


# constants
STORAGE_FOLDER = './storage/'
CANTUS_DB_FILE = STORAGE_FOLDER + 'chant_info.db'
DICTIONARY_MELODY = STORAGE_FOLDER + 'dictionary_melody.pkl'
DICTIONARY_LYRICS = STORAGE_FOLDER + 'dictionary_lyrics.pkl'

RECOMMENDATIONS_FLAG = False

MUSIC_MIN_SCORE = 0.7
LYRICS_MIN_SCORE = 0.25


# global variables
sequence_foralign = ''
max_alignments = 1

# local aligner
aligner = PairwiseAligner()
aligner.mode = 'local'
aligner.match_score = 1
aligner.mismatch_score = -0.5
aligner.open_gap_score = -1
aligner.extend_gap_score = -0.5



def read_omr(datapath):
    """Read result from OMR.
    """
    omrdata = []
    trans_folios = []

    badsubstrings = ["V.", ".", "*", "\n"]

    # prediction (coordinates) folder
    pred_path = os.path.join(datapath, 'predictions')

    # read data for each folio
    for file in os.listdir(pred_path):
        # folio/file name
        base_name = os.path.splitext(file)[0]

        # read coordinates table
        with open(os.path.join(pred_path, file), "r") as fileh:
            lines = fileh.readlines()
        
        # parse table and sort according to y_center
        table = [list(map(float, line.strip().split())) for line in lines]
        table = [entry for entry in table if int(entry[0]) == 12]
        table.sort(key=lambda x: x[2])

        # read transcription for each square
        trans_path = os.path.join(datapath, 'trans', base_name)

        if not os.path.exists(trans_path):
            omrdata.append({'name': base_name, 'table': table, 'trans': []})
            trans_folios.append('')
            continue

        trans_folio = [None] * len(os.listdir(trans_path))
        for transfile in os.listdir(trans_path):
            with open(os.path.join(trans_path, transfile), "r") as fileh:
                texto = fileh.read()

                # clean transcription
                for badsub in badsubstrings:
                    texto = texto.replace(badsub, "")
                texto = ' '.join(texto.split())

                num = int(re.search(r'\d+', transfile).group())
                trans_folio[num-1] = texto

        omrdata.append({'name': base_name, 'table': table, 'trans': trans_folio})
        trans_folios.append(' '.join(trans_folio))

    return omrdata, trans_folios



def create_omr_sequence(omrdata, trans_folios):
    """Create string sequence from OMR data.
    """
    seq_folio = []
    seq_y = []
    seq_box = []

    nboxes = 0

    for folio, omrfolio in enumerate(omrdata):
        for square, omrsquare  in enumerate(omrfolio['table']):
            aux = [omrsquare[2]] * (len(omrfolio['trans'][square]) + 1)
            seq_y.extend(aux)
            aux = [folio] * (len(omrfolio['trans'][square]) + 1)
            seq_folio.extend(aux)
            aux = [nboxes + square] * (len(omrfolio['trans'][square]) + 1)
            seq_box.extend(aux)
        nboxes = nboxes + square + 1

    seq_folio = seq_folio[:-1]
    seq_y = seq_y[:-1]
    seq_box = seq_box[:-1]
    sequence = ' '.join(trans_folios)

    return sequence, seq_folio, seq_y, seq_box



def clean_gabc(text):
    """Clean gabc melody for chant detection.
    """

    text = text.lower()

    # delete clef
    text = re.sub(r"(c\d|f\d|cb\d|fb\d)", "", text)

    # delete v
    text = text.replace('v', '')

    # preserve only pitches
    text = re.sub(r"[^a-m\s]", " ", text)

    # single space
    text = ' '.join(text.split())

    return text



def read_omr_music(datapath):
    """Read result from OMR (music version).
    """
    omrdata = []
    trans_folios = []

    badsubstrings = ["V.", ".", "*", "\n"]

    # prediction (coordinates) folder
    pred_path = os.path.join(datapath, 'predictions')

    # read data for each folio
    for file in os.listdir(pred_path):
        # folio/file name
        base_name = os.path.splitext(file)[0]

        # read coordinates table
        with open(os.path.join(pred_path, file), "r") as fileh:
            lines = fileh.readlines()
        
        # parse table and sort according to y_center
        table = [list(map(float, line.strip().split())) for line in lines]
        table = [entry for entry in table if int(entry[0]) == 12]
        table.sort(key=lambda x: x[2])

        # read transcription for each square
        trans_path = os.path.join(datapath, 'trans_music', base_name)

        if not os.path.exists(trans_path):
            omrdata.append({'name': base_name, 'table': table, 'trans': []})
            trans_folios.append('')
            continue

        trans_folio = [None] * len(os.listdir(trans_path))
        for transfile in os.listdir(trans_path):
            with open(os.path.join(trans_path, transfile), "r") as fileh:
                texto = fileh.read()

                # clean transcription
                texto = clean_gabc(texto)

                num = int(re.search(r'\d+', transfile).group())
                trans_folio[num-1] = texto

        omrdata.append({'name': base_name, 'table': table, 'trans': trans_folio})
        trans_folios.append(' '.join(trans_folio))

    return omrdata, trans_folios



def gabc_diff_encoding(text):
    """Differencial encoding for gabc.
    """

    letters = "abcdefghijklmnopqrstuvwxyz"
    result = []
    prev_char = None
    for char in text:
        if char == ' ':
            result.append(' ')
        else:
            if prev_char is None: # first letter
                result.append('')
            else:
                diff = ord(char) - ord(prev_char)
                if diff >= 0:
                    encoded_char = letters[diff % 26]  # Positivos en minúsculas
                else:
                    encoded_char = letters[(-diff) % 26].upper()  # Negativos en mayúsculas
                result.append(encoded_char)
            prev_char = char

    if len(result) > 1 and result[1] == ' ':
        result = result[2:]
    return ''.join(result)



def clean_volpiano(text):
    """Clean volpiano melody for chant detection and use differencial encoding.
    """

    text = text.lower()
    
    # erase i, I, y, Y, z and Z
    text = "".join(char for char in text if char not in "iIyYzZ")
    
    # replace 9 and ) by chr(ord('a')-1) (character before a)
    text = text.replace('9', '`').replace(')', '`')
    
    text = re.sub(r'-+', ' ', text)
    text = re.sub(r'[^a-z `]', '', text)
    text = re.sub(r'\s+', ' ', text).strip() # spaces to one space

    # diff coding
    valid_chars = "`abcdefghjklmnopqrstuvwxyz"
    char_to_index = {char: idx for idx, char in enumerate(valid_chars)}

    letters = "abcdefghijklmnopqrstuvwxyz"
    result = []
    prev_char = None
    for char in text:
        if char == ' ':
            result.append(' ')
        else:
            if prev_char is None: # first letter
                result.append('')
            else:
                # diff = ord(char) - ord(prev_char)
                diff = char_to_index[char] - char_to_index[prev_char]
                if diff >= 0:
                    encoded_char = letters[diff % 26]  # Positivos en minúsculas
                else:
                    encoded_char = letters[(-diff) % 26].upper()  # Negativos en mayúsculas
                result.append(encoded_char)
            prev_char = char

    if len(result) > 1 and result[1] == ' ':
        result = result[2:]
    return ''.join(result)



def get_melodies_cantusdatabase():
    """Retrieve all melodies in local DB
    """
    # load locally stored dictionary
    if os.path.exists(DICTIONARY_MELODY):
        with open(DICTIONARY_MELODY, 'rb') as archivo:
            id, cantusid, volpiano = pickle.load(archivo)
        return id, cantusid, volpiano

    # connect to database
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()

    # get full text
    cursor.execute('SELECT id, cantusid, volpiano FROM chant_cantusdb WHERE volpiano != ""')
    result = cursor.fetchall()
    conection.close()

    # convert to list
    id = list(item[0] for item in result)
    cantusid = list(item[1] for item in result)
    volpiano = list(item[2] for item in result)
    lenvolpiano = [0] * len(volpiano)

    # clean volpiano melodies
    for i, volp in enumerate(volpiano):
        volpiano[i] = clean_volpiano(volp)
        lenvolpiano[i] = len(volpiano[i].replace(' ', ''))

    # filter very short melodies
    MINLEN = 4
    id = list(item for item, lenv in zip(id, lenvolpiano) if lenv>=MINLEN)
    cantusid = list(item for item, lenv in zip(cantusid, lenvolpiano) if lenv>=MINLEN)
    volpiano = list(item for item, lenv in zip(volpiano, lenvolpiano) if lenv>=MINLEN)

    # store dictionary into file
    with open(DICTIONARY_MELODY, 'wb') as archivo:
        pickle.dump((id, cantusid, volpiano), archivo)

    return id, cantusid, volpiano



def get_chant_fulltext(clean=True):
    """Retrieve full text for all chants in local DB
    """
    # connect to database
    conection = sqlite3.connect(CANTUS_DB_FILE)
    cursor = conection.cursor()

    # get full text
    cursor.execute('SELECT text, id FROM chant')
    result = cursor.fetchall()
    conection.close()

    pattern = re.compile(r"///|\|\.\.\.|\(\.\.\.\)|\*|###|\.\.\.")
    pattern2 = re.compile(r"\(\.\.\.\)")
    incompleto = False
    dictionary = []
    cantusid = []

    # clean text
    for entry in result:
        canto = entry[0]
        id = entry[1]

        if not clean:
            dictionary.append(canto)
        else:
            if pattern2.search(canto):
                incompleto = True
            else:
                incompleto = False
            texto = re.sub(pattern, "", canto)
            texto = ' '.join(texto.split())
            if incompleto and len(texto.split()) == 1:
                continue
            dictionary.append(texto)
            cantusid.append(id)

    return dictionary, cantusid



def align_chant(canto):
    """Compute alignment between chant and sequence
    """
    score = []
    start = []
    end = []

    # max_alignments = 1

    if len(canto) == 0:
        return (score, start, end)

    alignments = aligner.align(sequence_foralign, canto)

    try:
        if len(alignments) == 0:
            return (score, start, end) 
    except:
        return (score, start, end)
    
    rangos = []

    for i, alignment in enumerate(alignments):
        if i >= max_alignments:
            break;
        # extract score and range
        scorei = alignment.score / len(canto)
        starti = alignment.aligned[0][0][0]
        endi = alignment.aligned[0][-1][-1]

        enrango = False
        for rango in rangos:
            if starti in rango or endi-1 in rango:
                enrango = True
                break;
        if enrango:
            break;
        rangos.append(range(starti, endi))

        score.append(scorei)
        start.append(starti)
        end.append(endi)

    return (score, start, end) 



def dpcore_chant(M):
    """
    Parameters:
    M (numpy.ndarray): Input score matrix .

    Returns:
    q (numpy.ndarray): Optimal path.
    D (numpy.ndarray): Dynamic programming matrix.
    phi (numpy.ndarray): Backtracking matrix.
    """

    # Transition cost
    C = 1.0

    # Convert to local cost
    M = 1 - M

    # Initialize variables
    rows, cols = M.shape
    D = np.zeros_like(M, dtype=float)
    phi = np.zeros_like(M, dtype=int)

    # Cell classes
    VACIO = 0
    BEGIN = 2
    FINAL = 3
    MIDDL = 1

    diffmatrix = np.diff(np.hstack([M, np.zeros((rows, 1))]), axis=1)
    MM = np.full_like(M, MIDDL, dtype=int)
    MM[diffmatrix > 0] = FINAL
    temp_matrix = np.hstack([np.zeros((rows, 1)), diffmatrix[:, :-1]])
    MM[temp_matrix < 0] = BEGIN
    MM[M == 1] = VACIO

    del diffmatrix

    # DP algorithm
    for t in tqdm(range(1, cols)):
        idxt = MM[:, t] != BEGIN
        D[idxt, t] = M[idxt, t] * C + D[idxt, t - 1]
        idxtt = np.where((MM[:, t - 1] == FINAL) | (MM[:, t - 1] == VACIO))[0]

        for j in np.where(~idxt)[0]:
            d = np.finfo(float).max
            tb = 0

            for jj in np.hstack([[j], idxtt]):
                d2 = M[j, t] * C + D[jj, t - 1]

                if d2 < d:
                    d = d2
                    tb = j - jj

            # Store result for this cell
            D[j, t] = d
            phi[j, t] = tb

    # Backtrack
    q = np.zeros(cols, dtype=int)
    idx = np.argmin(D[:, -1])
    q[-1] = idx
    for t in range(cols - 1, 0, -1):
        q[t - 1] = idx - phi[idx, t]
        idx = q[t - 1]

    return q, D, phi



def compute_similarity_hybrid(source, numfolios=-1):
    """Decide sequence of chants from a OMR transcribed source.
    """

    gc.collect()

    # Location of all source info
    omrdatapath = os.path.join('./data/', source)

    # Read OMR transcription (lyrics)
    omrdata, trans_folios = read_omr(omrdatapath)
    if numfolios < 0:
        numfolios = len(omrdata)
    omrdata = omrdata[0:numfolios]
    trans_folios = trans_folios[0:numfolios]

    sequence, seq_folio, seq_y, seq_box = create_omr_sequence(omrdata, trans_folios)

    # Read OMR transcription (music)
    omrdata_music, trans_folios_music = read_omr_music(omrdatapath)
    omrdata_music = omrdata_music[0:numfolios]
    trans_folios_music = trans_folios_music[0:numfolios]
    
    (sequence_music, seq_folio_music, seq_y_music, seq_box_music) = create_omr_sequence(
        omrdata_music, trans_folios_music)
    sequence_musicdiff = gabc_diff_encoding(sequence_music[0]+sequence_music)

    # Name of folios
    folios = os.listdir(os.path.join(omrdatapath, 'trans'))
    folios = folios[0:numfolios]

    # Melodies dictionary
    id_music, cantusid_music, dictionary_music = get_melodies_cantusdatabase()

    # Lyrics dictionary
    dictionary, cantusid = get_chant_fulltext()

    global sequence_foralign
    global max_alignments

    #--------------------------------------------------------------------
    # Alignment score matrix (music)
    rows = len(dictionary_music)
    cols = len(sequence_musicdiff)
    # matrix_music = np.zeros((rows, cols))
    
    print('')
    print('ALIGNING WITH MELODY/LYRICS DICTIONARIES ...')

    # Local alignment (music)
    sequence_foralign = sequence_musicdiff
    max_alignments = 1
    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.imap(align_chant, dictionary_music),
                            total=len(dictionary_music)))

    # Filter alignments (music)
    cantusid_musicfil = []
    result_music_score = []
    result_music_start = []
    result_music_end = []
    jj = 0
    for idx, (score, start, end) in tqdm(enumerate(results)): # chant
        chantadded = False
        for i, _ in enumerate(score):                         # alignment
            if score[i] < MUSIC_MIN_SCORE:
                continue
            # matrix_music[jj, start[i]:end[i]] = score[i]
            if not chantadded:
                cantusid_musicfil.append(cantusid_music[idx])
                result_music_score.append(score[i])
                result_music_start.append(start[i])
                result_music_end.append(end[i]-1)
                chantadded = True
        # if chantadded:
        #     jj = jj + 1
    # matrix_music = matrix_music[:jj,:]


    #--------------------------------------------------------------------
    # Alignment score matrix (lyrics)
    rows = len(dictionary)
    cols = len(sequence)
    matrix = np.zeros((rows, cols))

    # Local alignment (lyrics)
    sequence_foralign = sequence
    max_alignments = 10
    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.imap(align_chant, dictionary),
                            total=len(dictionary)))

    # Fill matrix (lyrics)
    cantusidfil = []
    dictionaryfil = []
    jj = 0
    for idx, (score, start, end) in tqdm(enumerate(results)): # chant
        chantadded = False
        for i, _ in enumerate(score):                         # alignment
            if score[i] <= LYRICS_MIN_SCORE:
                continue

            # check music alignment
            if cantusid[idx] in cantusid_musicfil:
                # idxm = cantusid_musicfil.index(cantusid[idx])

                idxms = [ii for ii, valor in enumerate(cantusid_musicfil) if valor == cantusid[idx]]
                puntmusica = 0
                for idxm in idxms:
                    if (
                        seq_box[start[i]] <= seq_box_music[result_music_end[idxm]]
                        and seq_box[end[i]-1] >= seq_box_music[result_music_start[idxm]]
                    ):
                        if result_music_score[idxm]>puntmusica:
                            puntmusica = result_music_score[idxm]
                if puntmusica > 0:
                    score[i] = 0.3*score[i] + 0.7*puntmusica

            matrix[jj, start[i]:end[i]] = score[i]
            if not chantadded:
                cantusidfil.append(cantusid[idx])
                dictionaryfil.append(dictionary[idx])
                chantadded = True
        if chantadded:
            jj = jj + 1
    matrix = matrix[:jj,:]


    print('')
    print('SEQUENCE DECISION (sorry, this is a bit slow right now, wait for patches) ...')

    # Decision
    q, _, _ = dpcore_chant(matrix)

    # Retrieve the indices corresponding to the unique rows in the optimal path
    unique_q, idx_q = np.unique(q, return_index=True)
    unique_q = unique_q[np.argsort(idx_q)]

    firstchar = idx_q[np.argsort(idx_q)]
    decision = [dictionaryfil[i] for i in unique_q]
    decision_id = [cantusidfil[i] for i in unique_q]


    def get_decision_info(q, p):
        """Get chant alignment info (cost, begin, end) from alignment path.
        """
        #global filtered_matrix

        len = matrix.shape[1]

        decision_cost = [None] * q.size
        decision_begin = [None] * q.size
        decision_last = [None] * q.size # this is actually last+1

        for i in range(0, p.size):
            qi = q[i]
            pi = p[i]
            c = pi
            while matrix[qi,c] == 0:
                c = c+1
            decision_begin[i] = c
            decision_cost[i] = matrix[qi,c]
            c = c+1
            while c < len and matrix[qi,c] > 0:
                c = c+1
            decision_last[i] = c

        return decision_begin, decision_last, decision_cost


    decision_begin, decision_last, decision_cost = get_decision_info(unique_q, firstchar)


    # find aligned melody
    decision_melody = []
    for decid, decbegin, declast in zip(decision_id, decision_begin, decision_last):
        if decid in cantusid_musicfil and False:
            idm = cantusid_musicfil.index(decid)
            melodia = sequence_music[result_music_start[idm]:result_music_end[idm]]
            decision_melody.append(melodia)
        else:
            notaini = seq_box_music.index(seq_box[decbegin])
            if seq_box[declast-1] == seq_box[-1]:
                notaend = len(sequence_music)
            else:
                notaend = seq_box_music.index(seq_box[declast-1]+1)
            melodia = sequence_music[notaini:notaend]
            decision_melody.append(melodia)


    # format output result
    result = format_result(decision, decision_melody, decision_id,
                           decision_begin, decision_last, decision_cost,
                           seq_folio, seq_y, folios, os.path.abspath(omrdatapath))
    
    suggested = []
    if RECOMMENDATIONS_FLAG:
        for res in result:
            ordered = find_chant_suggested(res['id'], (source, 'CD'))
            ordered = ordered[:len(res['id'])]
            ordered = [entry[0] for entry in ordered]
            suggested.append(ordered)

    # to HTML
    create_js(result, os.path.abspath(omrdatapath), suggested)
    webbrowser.open(f"file://{os.path.join(os.path.abspath(omrdatapath), 'index.html')}")

    return
