from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DAGS_DIR = ROOT_DIR / "dags"
INCLUDE_DIR = ROOT_DIR / 'include'
TEMPLATE_DIR = INCLUDE_DIR / 'templates'
INPUTS_DIR = INCLUDE_DIR / 'inputs'
DATA_DIR = ROOT_DIR / 'data'
UPLOAD_DIR = DATA_DIR / 'upload'
DOWNLOAD_DIR = DATA_DIR / 'download'


if __name__ == "__main__":
    print(ROOT_DIR)
    print(DATA_DIR)
    print(DOWNLOAD_DIR)
    print(UPLOAD_DIR)
    # print(INPUTS_DIR)
    # print(TEMPLATE_DIR)
    # print(INCLUDE_DIR)
    # print(DAGS_DIR)
