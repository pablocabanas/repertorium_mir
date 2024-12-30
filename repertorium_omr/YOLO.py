import os
import shutil
from ultralytics import YOLO

from repertorium_omr.utils import obtain_partition, obtain_processed_yaml

class YOLOv9c:
    def __init__(self, ckpt_path: str = None):
        if ckpt_path is not None:
            self.model = YOLO(ckpt_path)
        else:
            self.model = YOLO('yolov9c.pt')

    def __name__(self):
        return 'YOLOv9c'

    def evaluate(self, folder: str):
        data = obtain_processed_yaml(folder)
        data['path'] = os.path.abspath(data['path'])
        images = obtain_partition(data, 'test')
        predictions_folder = os.path.join(data['path'], 'predictions')
        if os.path.exists(predictions_folder):
            shutil.rmtree(predictions_folder)

        for image in images:
            self.model.predict(image, save_txt=True, project="",name=predictions_folder,
                               imgsz=512, exist_ok=True)

        # Extract the predictions to parent folder
        parent_folder = os.path.join(data['path'], 'predictions')
        predictions_folder = os.path.join(parent_folder, 'labels')
        if not os.path.exists(predictions_folder):
            return
        for file in os.listdir(predictions_folder):
            shutil.move(os.path.join(predictions_folder, file), parent_folder)
        shutil.rmtree(predictions_folder)
        
    def save(self, results):
        pass
    