import tools
import pandas as pd

excel_data = '../data/data_engineer_raw_data.xlsx'

df = pd.read_excel(excel_data, sheet_name=None)

for key in df.keys():
    sheet = pd.read_excel(excel_data, sheet_name=key)
    csv_name = '../data/%s.csv' % key
    sheet.to_csv(csv_name, index=False)
    tools.upload_file(csv_name, 'aspencapital', object_name=None)



