from typing import Iterator
from bio_autorun.datasets.generic import Dataset
from bio_autorun.msa import MSA, MSACategory
from pathlib import Path


class YhDataset(Dataset):
    def __init__(self, msa_dict: dict[MSACategory, MSA]):
        self.msa_list: list[MSA] = []
        self.all_list: list[tuple[str, MSA, str]] = []
        for category, subdata_dirs in msa_dict.items():
            for subdata_dir in subdata_dirs:
                subdata_dir = Path(subdata_dir)
                for msa_dir in subdata_dir.iterdir():
                    if msa_dir.is_dir():
                        msa_file = (msa_dir / f"data.{msa_dir.name}")
                        tree_file = (msa_dir / f"tree.{msa_dir.name}")
                        if msa_file.exists() and tree_file.exists():
                            model_file = (msa_dir / f"model.{msa_dir.name}")
                            model_name = model_file.read_text().strip() if model_file.exists() else None
                            msa = MSA(f"{subdata_dir.name}_{msa_dir.name}", str(msa_file.absolute()), category=category, model_name=model_name)
                            self.msa_list.append(msa)
                            self.all_list.append((subdata_dir.name, msa, str(tree_file.absolute())))
    
    def filter(self, **kwargs) -> Iterator[tuple[MSA, str]]:
        for subdata, msa, tree_path in self.all_list:
            if len(kwargs) == 0:
                yield msa, tree_path
            else:
                ok = True
                for key, value in kwargs.items():
                    if key == "subdata" and subdata != value:
                        ok = False
                    if key == "category" and msa.category != value:
                        ok = False
                if ok:
                    yield msa, tree_path
    
    def __getitem__(self, index: int) -> MSA:
        return self.msa_list[index]
    
    def __len__(self) -> int:
        return len(self.msa_list)
