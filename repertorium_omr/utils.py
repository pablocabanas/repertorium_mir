import os
import yaml
import numpy as np
import gradio as gr
from typing import Optional

def obtain_processed_yaml(folder: str):
    if not os.path.isdir(folder):
        raise gr.Error(f"{folder} is not a directory", duration=5)
    
    yaml_file = [f for f in os.listdir(folder) if f.endswith('.yaml')][0]

    with open(os.path.join(folder, yaml_file), 'r') as f:
        data = yaml.safe_load(f)
    return data

def obtain_partition(data: dict, partition: str):
    try:
        path_images = os.path.join(data['path'], data[partition])
        images = [os.path.join(path_images, image) for image in os.listdir(path_images)]
    except Exception as e:
        raise gr.Error(e, duration=5)
    
    return images

def unravel_data(data: dict):
    images = []
    labels = []

    for partition in ['train', 'val', 'test']:
        path_images = os.path.join(data['path'], data[partition])
        path_labels = path_images.replace('images', 'labels')
        for image in os.listdir(path_images):
            images.append(os.path.join(path_images, image))
            labels.append(os.path.join(path_labels, image.replace('.png', '.txt')))

    return images, labels

def generate_random_color(classes: Optional[dict]):
    if classes is not None:
        return {c: np.random.randint(0, 255, 3).tolist() for c in classes.keys()}