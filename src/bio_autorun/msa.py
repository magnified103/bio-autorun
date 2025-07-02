import os
import sys

from bio_autorun.iqtree.settings import Settings

if sys.version_info < (3, 11):
    from enum import Enum

    class MSACategory(str, Enum):
        dna = "dna"
        protein = "protein"

else:
    from enum import StrEnum

    class MSACategory(StrEnum):
        dna = "dna"
        protein = "protein"


def treebase_load(settings: Settings):
    msa_list: list[MSA] = []
    for name in os.listdir(settings.data_dir):
        if name.endswith(".phy"):
            msa_list.append(MSA(name, os.path.join(settings.data_dir, name)))
    return msa_list


def yh_load(settings: Settings):
    msa_list: list[MSA] = []
    for name in os.listdir(settings.data_dir):
        if name.endswith(".phy"):
            msa_list.append(
                MSA(name, os.path.join(settings.data_dir, name), MSACategory.dna)
            )
    return msa_list


def treebase_classifier(name) -> MSACategory:
    if name.startswith("dna"):
        return MSACategory.dna
    return MSACategory.protein


class MSA:
    def __init__(
        self,
        name=None,
        path=None,
        category: MSACategory = None,
        classifier=treebase_classifier,
        model_name=None,
    ):
        self.name = name
        self.path = path
        self.model_name = model_name
        self.category: MSACategory = category
        if category is None and classifier is not None:
            self.category = classifier(name)

    def __hash__(self):
        return hash(self.name)
