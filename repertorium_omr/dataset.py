import json
import os
from torch.utils.data import Dataset

from repertorium_omr.data_preprocessing import (
    preprocess_image_from_file
)

################################################################################################ Single-source:


class CTCDataset(Dataset):
    def __init__(
        self,
        name,
        img_folder,
        encoding_type="char",
    ):
        self.name = name
        self.encoding_type = encoding_type

        # Get image paths and transcripts
        self.X = self.get_images_filepaths(img_folder)

        # Check and retrieve vocabulary
        vocab_name = f"w2i_{self.encoding_type}.json"
        vocab_folder = os.path.join(os.path.join("data", self.name), "vocab")
        os.makedirs(vocab_folder, exist_ok=True)
        self.w2i_path = os.path.join(vocab_folder, vocab_name)
        self.w2i, self.i2w = self.check_and_retrieve_vocabulary()

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        img_path = self.X[idx]
        
        if self.name == "aligned":
            unfolding = True
        else:
            unfolding = False
        
        x = preprocess_image_from_file(self.X[idx], unfolding=unfolding)
        return x, img_path

    def get_images_filepaths(
        self, img_directory
    ):
        images = []

        # Open each file in the directory
        img_files = os.listdir(img_directory)

        for img_file in img_files:
            file_name_without_extension, _ = os.path.splitext(img_file)
            img_path = os.path.join(img_directory, img_file)

            if os.path.exists(img_path):
                images.append(img_path)

        return images

    def check_and_retrieve_vocabulary(self):
        w2i = {}
        i2w = {}

        if os.path.isfile(self.w2i_path):
            with open(self.w2i_path, "r") as file:
                w2i = json.load(file)
            i2w = {v: k for k, v in w2i.items()}
        else:
            print("Vocabulary not found.")

        return w2i, i2w