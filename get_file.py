import json
import os.path

import pandas as pd
import requests


def down_file(path, file):
    headers = {

        #"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    }
    print(f"开始下载{file}")
    save_path = os.path.join("result_plus2", path)
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    url = file.get("file_url")
    file_name = file.get("file_name").replace("\\", "")
    content = requests.get(url, headers=headers)
    content.encoding=content.apparent_encoding
    content=content.content
    print(content)
    with open(os.path.join(save_path, file_name), 'wb') as fp:
        fp.write(content)


def read_csv():
    fp = pd.read_excel(r"E:\alpm\fenbao\std_hefei_final.xlsx", header=0)
    for i, row in fp.iterrows():
        path = row['url'].rsplit("/", 1)[-1].replace(".htm", "")
        files = json.loads(row['attachments'].replace('""', "'"))
        if isinstance(files, str):
            files = eval(files)
        for file in files:
            down_file(path, file)
    # with open(file="std_hefei_final.xlsx",encoding='utf-8',mode='r') as fp:
    #     for data in fp:
    #         #data = i.split(chr(9578))
    #         print(data)
    #         if data[1] == "url":
    #             continue
    #         path = data[1].rsplit("/", 1)[-1].replace(".htm", "")
    #         files = json.loads(data[47].replace('""', "'"))
    #         #注意改为47
    #         if isinstance(files, str):
    #             files = eval(files)
    #         for file in files:
    #             down_file(path, file)


if __name__ == '__main__':
    read_csv()
