from bio_autorun.datasets.generic import Dataset
from bio_autorun.msa import MSA
import os


class TreebaseDataset(Dataset):
    def __init__(self, data_dir: str):
        self.msa_list: list[MSA] = []
        for name in os.listdir(data_dir):
            if name.endswith(".phy"):
                self.msa_list.append(MSA(name[:-4], os.path.abspath(os.path.join(data_dir, name))))

    def __getitem__(self, index: int) -> MSA:
        return self.msa_list[index]

    def __len__(self) -> int:
        return len(self.msa_list)


class TreebaseFastaDataset(Dataset):
    def __init__(self, data_dir: str):
        self.msa_list: list[MSA] = []
        for name in os.listdir(data_dir):
            if name.endswith(".fasta"):
                self.msa_list.append(MSA(name[:-6], os.path.abspath(os.path.join(data_dir, name))))

    def __getitem__(self, index: int) -> MSA:
        return self.msa_list[index]

    def __len__(self) -> int:
        return len(self.msa_list)
