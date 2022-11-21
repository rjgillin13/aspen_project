import scripts.tools as tools
import pandas as pd
import os
import pathlib

filepath = os.path.join(os.getcwd(), pathlib.Path("data/data_engineer_raw_data.xlsx"))

df = pd.read_excel(filepath, sheet_name=None)

for key in df.keys():
    sheet = pd.read_excel(filepath, sheet_name=key, skiprows=0)
    csv_name = 'data/%s.csv' % key
    sheet.to_csv(csv_name, index=False, header=False)
    tools.upload_file(csv_name, 'aspencapital', object_name=None)

