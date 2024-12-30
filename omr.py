import sys
import os
import gc
import random

import shutil
import numpy as np
import torch
from lightning.pytorch import Trainer
from lightning.pytorch.loggers.wandb import WandbLogger
from torch.utils.data import DataLoader
from PIL import Image
import yaml

from repertorium_omr.YOLO import YOLOv9c
from repertorium_omr.dataset import CTCDataset
from repertorium_omr.model import CTCTrainedCRNN
from repertorium_omr.model import LightningE2EModelUnfolding


# constants
CKPT_YOLO = './weights/best.pt'
ds_name = 'lyrics'
checkpoint_path = './weights/repertorium_' + ds_name + '_char_crnn_greedy.ckpt'
ds_name_music = 'music'
checkpoint_path_music = './weights/repertorium_' + ds_name_music + '_char_crnn_greedy.ckpt'
ds_name_aligned = 'aligned'
checkpoint_path_aligned = './weights/repertorium_' + ds_name_aligned + '_char_crnn_greedy.ckpt'


# Seed
random.seed(42)
np.random.seed(42)
torch.manual_seed(42)

# Deterministic behavior
torch.backends.cudnn.benchmark = False
torch.backends.cudnn.deterministic = True



def prepare_folder_for_omr(input_folder, package_folder):
    """Prepare images in a folder for OMR.
    """
    max_pixels = 1_000_000

    # create folder/file tree
    os.makedirs(package_folder, exist_ok=True)
    os.makedirs(os.path.join(package_folder, 'images', 'test'), exist_ok=True)
    os.makedirs(os.path.join(package_folder, 'images', 'train'), exist_ok=True)
    os.makedirs(os.path.join(package_folder, 'images', 'validation'), exist_ok=True)
    yamldata = {
        'names': None,
        'path': package_folder,
        'test': 'images/test',
        'train': 'images/train',
        'val': 'images/validation'
    }
    with open(os.path.join(package_folder, 'dataset.yaml'), "w") as yaml_file:
        yaml.dump(yamldata, yaml_file, default_flow_style=False)

    img_folder = os.path.join(package_folder, 'images', 'test')

    # convert files
    for file in os.listdir(input_folder):
        # read image file
        try:
            imagen = Image.open(os.path.join(input_folder, file))
        except Exception as e:
            continue

        # resize image
        if imagen.size[0] * imagen.size[1] > max_pixels:
            scale = (max_pixels / (imagen.size[0] * imagen.size[1])) ** 0.5
            new_size = (int(imagen.size[0] * scale), int(imagen.size[1] * scale))
            imagen = imagen.resize(new_size, Image.Resampling.LANCZOS)

        # save in PNG
        base_name = os.path.splitext(os.path.basename(file))[0]
        imagen.save(os.path.join(img_folder, base_name + '.png'), format='PNG')

    return


def image_crop_yolo(image_path, yolo_path, output_folder):
    """Crop image according to yolo prediction table (lyrics).
    """
    # load image
    image = Image.open(image_path)
    image_width, image_height = image.size

    # read YOLO table
    with open(yolo_path, "r") as file:
        lines = file.readlines()

    # parse table and sort according to y_center
    table = [list(map(float, line.strip().split())) for line in lines]
    table = [entry for entry in table if int(entry[0]) == 12]
    table.sort(key=lambda x: x[2])

    # center blocks around the lyrics and convert to xmin, ymin
    for i, entry in enumerate(table):
        entry[1] = max(0, entry[1] - entry[3]/2)
        # entry[2] = max(0, entry[2] + 3*entry[4]/10) # check
        if i < len(table)-1:
            entry[4] = table[i+1][2] - entry[2]
            # entry[4] = table[i+1][2] - 3*table[i+1][4]/10 - entry[2]

    # output subfolder
    base_name = os.path.splitext(os.path.basename(image_path))[0]
    output_folder = os.path.join(output_folder, base_name)
    os.makedirs(output_folder, exist_ok=True)

    # process each entry
    for idx, (_, x_minc, y_minc, width, height) in enumerate(table):
        
        # pixels coordinates
        x_max = int((x_minc + width) * image_width)
        y_max = int((y_minc + height) * image_height)
        x_min = int(x_minc * image_width)
        y_min = int(y_minc * image_height)

        x_min = max(0, x_min)
        y_min = max(0, y_min)
        x_max = min(image_width, x_max)
        y_max = min(image_height, y_max)

        # crop
        cropped_image = image.crop((x_min, y_min, x_max, y_max))

        # save
        output_path = os.path.join(output_folder, 
                                   f"recorte_{idx+1}.{image.format.lower()}")
        cropped_image.save(output_path)


def image_crop_yolo_music(image_path, yolo_path, output_folder):
    """Crop image according to yolo prediction table (music).
    """
    # load image
    image = Image.open(image_path)
    image_width, image_height = image.size

    # read YOLO table
    with open(yolo_path, "r") as file:
        lines = file.readlines()

    # parse table and sort according to y_center
    table = [list(map(float, line.strip().split())) for line in lines]
    table = [entry for entry in table if int(entry[0]) == 12]
    table.sort(key=lambda x: x[2])

    # convert to xmin, ymin
    for i, entry in enumerate(table):
        entry[1] = max(0, entry[1] - entry[3]/2)
        entry[2] = max(0, entry[2] - entry[4]/2)

    # output subfolder
    base_name = os.path.splitext(os.path.basename(image_path))[0]
    output_folder = os.path.join(output_folder, base_name)
    os.makedirs(output_folder, exist_ok=True)

    # process each entry
    for idx, (_, x_minc, y_minc, width, height) in enumerate(table):
        
        # pixels coordinates
        x_max = int((x_minc + width) * image_width)
        y_max = int((y_minc + height) * image_height)
        x_min = int(x_minc * image_width)
        y_min = int(y_minc * image_height)

        x_min = max(0, x_min)
        y_min = max(0, y_min)
        x_max = min(image_width, x_max)
        y_max = min(image_height, y_max)

        # crop
        cropped_image = image.crop((x_min, y_min, x_max, y_max))

        # save
        output_path = os.path.join(output_folder, 
                                   f"recorte_{idx+1}.{image.format.lower()}")
        cropped_image.save(output_path)


def image_crop_yolo_aligned(image_path, yolo_path, output_folder):
    """Crop image according to yolo prediction table (aligned).
    """
    # load image
    image = Image.open(image_path)
    image_width, image_height = image.size

    # read YOLO table
    with open(yolo_path, "r") as file:
        lines = file.readlines()

    # parse table and sort according to y_center
    table = [list(map(float, line.strip().split())) for line in lines]
    table = [entry for entry in table if int(entry[0]) == 12]
    table.sort(key=lambda x: x[2])

    # convert to xmin, ymin
    for i, entry in enumerate(table):
        entry[1] = max(0, entry[1] - entry[3]/2)
        entry[2] = max(0, entry[2] - entry[4]/2)
        if i < len(table)-1:
            entry[4] = table[i+1][2] - entry[2] - table[i+1][4]/4
        else:
            entry[4] = min(1, entry[4] + entry[4]/2)
        # entry[4] = min(1, entry[4] + entry[4]/2)

    # output subfolder
    base_name = os.path.splitext(os.path.basename(image_path))[0]
    output_folder = os.path.join(output_folder, base_name)
    os.makedirs(output_folder, exist_ok=True)

    # process each entry
    for idx, (_, x_minc, y_minc, width, height) in enumerate(table):
        
        # pixels coordinates
        x_max = int((x_minc + width) * image_width)
        y_max = int((y_minc + height) * image_height)
        x_min = int(x_minc * image_width)
        y_min = int(y_minc * image_height)

        x_min = max(0, x_min)
        y_min = max(0, y_min)
        x_max = min(image_width, x_max)
        y_max = min(image_height, y_max)

        # crop
        cropped_image = image.crop((x_min, y_min, x_max, y_max))

        # rotate
        #rotated_image = cropped_image.transpose(Image.ROTATE_270)
        rotated_image = cropped_image

        # save
        output_path = os.path.join(output_folder, 
                                   f"recorte_{idx+1}.{image.format.lower()}")
        rotated_image.save(output_path)


def process_omr(source):

    print('--------------------------------------------------------')
    print('OMR')
    print('--------------------------------------------------------')

    current_folder = os.getcwd()
    
    if os.path.exists(source): # raw image folder
        print(f'PROCESSING folder {source}')
        input_folder = source
        source = os.path.split(source)[-1]
        package_folder = os.path.join(current_folder, 'data', source)
        prepare_folder_for_omr(input_folder, package_folder)
    elif len(source)>0: # package name
        print(f'PROCESSING package {source}')
        package_folder = os.path.join(current_folder, 'data', source)
    else:
        return


    # ---- LAYOUT PROCESSING ---------------------------------
    print('DETECTING LAYOUT ...')
    # Detection model
    model = YOLOv9c(CKPT_YOLO)
    # Predictions of detection model
    model.evaluate(package_folder)


    # ---- TRAINSCRIPTION ---------------------------------
    folios =  os.listdir(os.path.join(package_folder, 'images', 'test'))
    print(f'\nTRANSCRIBING {len(folios)} folios ...')

    for folio in folios:

        folio = os.path.splitext(folio)[0]
        print(f'\nFOLIO {folio} ...\n')

        gc.collect()
        torch.cuda.empty_cache()
    
        img_folder = os.path.join(package_folder, 'cropped', folio)
        img_folder_music = os.path.join(package_folder, 'cropped_music', folio)
        img_folder_aligned = os.path.join(package_folder, 'cropped_aligned', folio)

        if not os.path.exists(os.path.join(package_folder, 'predictions', folio + '.txt')):
            continue
        # Crop squares (lyrics, music)
        image_crop_yolo(os.path.join(package_folder, 'images', 'test', folio + '.png'),
                        os.path.join(package_folder, 'predictions', folio + '.txt'),
                        os.path.join(package_folder, 'cropped')
                        )
        image_crop_yolo_music(os.path.join(package_folder, 'images', 'test', folio + '.png'),
                              os.path.join(package_folder, 'predictions', folio + '.txt'),
                              os.path.join(package_folder, 'cropped_music')
                              )
        image_crop_yolo_aligned(os.path.join(package_folder, 'images', 'test', folio + '.png'),
                                os.path.join(package_folder, 'predictions', folio + '.txt'),
                                os.path.join(package_folder, 'cropped_aligned')
                                )


        # ----------- LYRICS ---------------------------------
        # Dataset for transcription
        test_ds = CTCDataset(
            name=ds_name,
            img_folder=img_folder,
            encoding_type='char',
        )
        test_loader = DataLoader(
            test_ds, batch_size=1, shuffle=False, num_workers=20
        )  # prefetch_factor=2

        # Transcription model (lyrics)
        model_lyrics = CTCTrainedCRNN.load_from_checkpoint(
                checkpoint_path, ctc='greedy', ytest_i2w=test_ds.i2w, ds_name=ds_name
                )
        model_lyrics.freeze()

        # Test: automatically auto-loads the best weights from the previous run
        trainer = Trainer(
            precision="16-mixed",
        )
        trainer.test(model_lyrics, dataloaders=test_loader)

        # Move the predictions
        predictions_folder = os.path.join('.', 'predictions', ds_name)
        output_folder = os.path.join(package_folder, 'trans', folio)
        os.makedirs(output_folder, exist_ok=True)
        for file in os.listdir(predictions_folder):
            shutil.move(os.path.join(predictions_folder, file),
                        os.path.join(output_folder, file))
        shutil.rmtree(predictions_folder)


        # ----------- MUSIC ---------------------------------
        # Dataset for transcription
        test_ds = CTCDataset(
            name=ds_name_music,
            img_folder=img_folder_music,
            encoding_type='char',
        )
        test_loader = DataLoader(
            test_ds, batch_size=1, shuffle=False, num_workers=20
        )  # prefetch_factor=2

        # Transcription model (music)
        model_music = CTCTrainedCRNN.load_from_checkpoint(
                checkpoint_path_music, ctc='greedy', ytest_i2w=test_ds.i2w, ds_name=ds_name_music
                )
        model_music.freeze()

        # Test: automatically auto-loads the best weights from the previous run
        trainer = Trainer(
            precision="16-mixed",
        )
        trainer.test(model_music, dataloaders=test_loader)

        # Move the predictions
        predictions_folder = os.path.join('.', 'predictions', ds_name_music)
        output_folder = os.path.join(package_folder, 'trans_music', folio)
        os.makedirs(output_folder, exist_ok=True)
        for file in os.listdir(predictions_folder):
            shutil.move(os.path.join(predictions_folder, file),
                        os.path.join(output_folder, file))
        shutil.rmtree(predictions_folder)


        # ----------- ALIGNED ---------------------------------
        # Dataset for transcription
        test_ds = CTCDataset(
            name=ds_name_aligned,
            img_folder=img_folder_aligned,
            encoding_type='char',
        )
        test_loader = DataLoader(
            test_ds, batch_size=1, shuffle=False, num_workers=20
        )  # prefetch_factor=2

        # Transcription model
        model_hybrid = LightningE2EModelUnfolding.load_from_checkpoint(
                checkpoint_path_aligned, ctc='greedy', ytest_i2w=test_ds.i2w
                )
        model_hybrid.freeze()

        # Test: automatically auto-loads the best weights from the previous run
        trainer = Trainer(
            precision="16-mixed",
        )
        trainer.test(model_hybrid, dataloaders=test_loader)

        # Move the predictions
        predictions_folder = os.path.join('.', 'predictions', ds_name_aligned)
        output_folder = os.path.join(package_folder, 'trans_aligned', folio)
        os.makedirs(output_folder, exist_ok=True)
        for file in os.listdir(predictions_folder):
            shutil.move(os.path.join(predictions_folder, file),
                        os.path.join(output_folder, file))
        shutil.rmtree(predictions_folder)

    return source
