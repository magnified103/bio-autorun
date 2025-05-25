from bio_autorun.datasets.generic import Dataset
from bio_autorun.msa import MSA
import os


class TreebaseDataset(Dataset):
    def __init__(self, data_dir: str):
        self.msa_list: list[MSA] = []
        for name in os.listdir(data_dir):
            if name.endswith(".phy"):
                self.msa_list.append(MSA(name, os.path.join(data_dir, name)))
    
    def get_msa_list(self):
        return list(self.msa_list)
