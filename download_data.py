import pandas as pd

url = 'https://raw.githubusercontent.com/gchandra10/filestorage/main/sales_100.csv'
df = pd.read_csv(url)
df.to_csv('./data/bronze_sales.csv', index=False)
