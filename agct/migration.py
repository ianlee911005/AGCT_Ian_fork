
from repo_loader import RepositoryLoader

LEGACY_DATA_FOLDERS = {
    "CANCER": "TGCA.V1/data_new/cancer",
    "ADRD": "TGCA.V1/data_new/AD",
    "CHD": "TGCA.V1/data_new/CHD",
    "DDD": "TGCA.V1/data_new/DDD",
    "ASD": "TGCA.V1/data_new/ASD",
}
LEGACY_CANCER_VARIANT_FILES = [
    {"source": "HOTSPOT", "label": 1, "file": "MSK_hotspot.csv"},
    {"source": "MSK_PASSENGER", "label": 0, "file": "MSK_passenger.csv"},
    {"source": "TCGA_PASSENGER", "label": 0, "file": "TCGA_passenger.csv"},
]
LEGACY_ADRD_VARIANT_FILES = [
    {"source": "ADRD", "label": 1, "file": "ADRD_case.csv"},
    {"source": "ADRD", "label": 0, "file": "ADRD_control.csv"}
]
LEGACY_CHD_VARIANT_FILES = [
    {"source": "CHD", "label": 1, "file": "CHD_case.csv"},
    {"source": "CHD", "label": 0, "file": "CHD_control.csv"}
]
LEGACY_DDD_VARIANT_FILES = [
    {"source": "DDD", "label": 1, "file": "DDD_case.csv"},
    {"source": "DDD", "label": 0, "file": "DDD_control.csv"}
]
LEGACY_ASD_VARIANT_FILES = [
    {"source": "ASD", "label": 1, "file": "ASD_case.csv"},
    {"source": "ASD", "label": 0, "file": "ASD_control.csv"}
]
LEGACY_VARIANT_FILES = {
    "CANCER": LEGACY_CANCER_VARIANT_FILES,
    "ADRD": LEGACY_ADRD_VARIANT_FILES,
    "CHD": LEGACY_CHD_VARIANT_FILES,
    "DDD": LEGACY_DDD_VARIANT_FILES,
    "ASD": LEGACY_ASD_VARIANT_FILES
}


def migrate_task_files(loader: RepositoryLoader, task: str):
    for file in LEGACY_VARIANT_FILES[task]:
        loader.generate_filter_cluster(task)
<<<<<<< HEAD
        #loader.load_variant_file("hg38", task, file["file"],
        #                         LEGACY_DATA_FOLDERS[task],
        #                         file["source"], file["label"],
        #                         "hg19", "hg18")
=======
        loader.load_variant_file("hg38", task, file["file"],
                                 LEGACY_DATA_FOLDERS[task],
                                 file["source"], file["label"],
                                 "hg19", "hg18")
>>>>>>> 8e4abe8e4a744ceebfc60266b22ef0b56c15de4b


loader = RepositoryLoader()
'''
loader.init_variant_task()
loader.init_variant_effect_source()
migrate_task_files(loader, "CANCER")
migrate_task_files(loader, "ADRD")
migrate_task_files(loader, "CHD")
migrate_task_files(loader, "DDD")
migrate_task_files(loader, "ASD")
'''
<<<<<<< HEAD
#loader.generate_filter_cluster('CLINVAR')
loader.load_clinvar("hg38", 'CLINVAR', 
                        'clinvar.csv',
                        'CLINVAR',
                        "hg19", "hg18")
=======
loader.load_clinvar("hg38", 'CLINVAR', 
                        'clinvar.csv',
                        'CLINVAR',
                        "hg19", "hg18")
>>>>>>> 8e4abe8e4a744ceebfc60266b22ef0b56c15de4b
