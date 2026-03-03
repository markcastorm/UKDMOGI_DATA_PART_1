import pandas as pd

# Read Excel without assuming header
df = pd.read_excel('downloads/20260113_121110/20260113 - Outright Gilt Issuance Calendar.xls',
                   engine='xlrd', header=None)

print(f'File shape: {df.shape}')
print('\nFirst 20 rows:\n')

for i in range(min(20, len(df))):
    row_values = df.iloc[i].tolist()
    # Show first 6 columns
    print(f'Row {i:2d}: {row_values[:6]}')
