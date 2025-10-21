import pandas as pd
from pandas import read_excel

# Dummy DataFrame 1
df1 = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['New York', 'Los Angeles', 'Chicago']
})

# Dummy DataFrame 2
df2 = pd.DataFrame({
    'id': [4, 5, 6],
    'name': ['David', 'Eva', 'Frank'],
    'age': [28, 32, 40],
    'city': ['Houston', 'Seattle', 'Boston']
})


excel = "test.xlsx"

dftest = read_excel(excel)

df_combined = pd.concat([dftest, df2], ignore_index=True)

df_combined.to_excel(excel, index=False, sheet_name="Combined")
