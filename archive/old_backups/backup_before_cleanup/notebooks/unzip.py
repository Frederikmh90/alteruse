import zipfile

path_to_zip_file = "../data/Kantar_download_280725_andetforsøg.zip"


directory_to_extract_to = "../data/Kantar_download_280725_andetforsøg"

with zipfile.ZipFile(path_to_zip_file, "r") as zip_ref:
    zip_ref.extractall(directory_to_extract_to)
