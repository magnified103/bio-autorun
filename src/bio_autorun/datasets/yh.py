from bio_autorun.datasets.generic import Dataset
from bio_autorun.msa import MSA, MSACategory
from pathlib import Path


class YhDataset(Dataset):
    def __init__(self, msa_dict: dict[MSACategory, MSA]):
        self.msa_list: list[MSA] = []
        for category, data_dirs in msa_dict.items():
            for data_dir in data_dirs:
                data_dir = Path(data_dir)
                for msa_dir in data_dir.iterdir():
                    if msa_dir.is_dir() and (msa_dir / f"data.{msa_dir.name}").exists():
                        self.msa_list.append(MSA(f"{data_dir.name}_{msa_dir.name}", str((msa_dir / f"data.{msa_dir.name}").absolute()), category=category))
    
    def __getitem__(self, index: int) -> MSA:
        return self.msa_list[index]
    
    def __len__(self) -> int:
        return len(self.msa_list)
