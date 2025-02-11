
from repo_loader import RepositoryLoader

LEGACY_DATA_FOLDERS = {
    "cancer": "TGCA.V1/data_new/cancer",
    "adrd": "TGCA.V1/data_new/AD",
    "chd": "TGCA.V1/data_new/CHD",
    "ddd": "TGCA.V1/data_new/DDD",
    "asd": "TGCA.V1/data_new/ASD",
}
LEGACY_CANCER_VARIANT_FILES = [
    {"source": "HOTSPOT", "label": 1, "file": "MSKhotspot.txt"},
    {"source": "MSK_PASSENGER", "label": 0, "file": "MSK_passenger.txt"},
]
LEGACY_ADRD_VARIANT_FILES = [
    {"source": "ADRD", "label": 1, "file": "ADcase.txt"},
    {"source": "ADRD", "label": 0, "file": "ADcontrol.txt"}
]
LEGACY_CHD_VARIANT_FILES = [
    {"source": "CHD", "label": 1, "file": "CHDcase.txt"},
    {"source": "CHD", "label": 0, "file": "CHDcontrol.txt"}
]
LEGACY_DDD_VARIANT_FILES = [
    {"source": "DDD", "label": 1, "file": "DDDcase.txt"},
    {"source": "DDD", "label": 0, "file": "DDDcontrol.txt"}
]
LEGACY_ASD_VARIANT_FILES = [
    {"source": "ASD", "label": 1, "file": "ASDcase.txt"},
    {"source": "ASD", "label": 0, "file": "ASDcontrol.txt"}
]
LEGACY_VARIANT_FILES = {
    "cancer": LEGACY_CANCER_VARIANT_FILES,
    "adrd": LEGACY_ADRD_VARIANT_FILES,
    "chd": LEGACY_CHD_VARIANT_FILES,
    "ddd": LEGACY_DDD_VARIANT_FILES,
    "asd": LEGACY_ASD_VARIANT_FILES
}


def migrate_task_files(loader: RepositoryLoader, task: str):
    for file in LEGACY_VARIANT_FILES[task]:
        loader.load_variant_file("hg38", task, file["file"],
                                 LEGACY_DATA_FOLDERS[task],
                                 file["source"], file["label"],
                                 "hg19", "hg18")


loader = RepositoryLoader()

loader.init_variant_task()
'''
loader.init_variant_effect_source()
migrate_task_files(loader, "cancer")
migrate_task_files(loader, "adrd")
migrate_task_files(loader, "chd")
migrate_task_files(loader, "ddd")
migrate_task_files(loader, "asd")
'''

loader.load_clinvar("hg38", 'clinvar', 
                        'clinvar.csv',
                        'clinvar',
                        "hg19", "hg18")