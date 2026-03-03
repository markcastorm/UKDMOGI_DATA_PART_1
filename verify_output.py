import pandas as pd
import os

print("Files in latest folder:")
files = os.listdir('output/latest')
for f in files:
    size = os.path.getsize(os.path.join('output/latest', f))
    print(f'  {f} ({size:,} bytes)')

print('\n' + '='*70)
print('DATA FILE VERIFICATION')
print('='*70)

df_data = pd.read_excel('output/latest/UKDMOGI_DATA_PART_1_20260113.xlsx', header=None)
print(f'\nTotal rows: {len(df_data)}')
print(f'Total columns: {len(df_data.columns)}')

print('\nFirst 7 rows (headers + first 5 data rows):')
for i in range(min(7, len(df_data))):
    print(f'Row {i}: {df_data.iloc[i, 0]} | {df_data.iloc[i, 1]}')

print('\nLast 5 rows:')
for i in range(max(0, len(df_data)-5), len(df_data)):
    print(f'Row {i}: {df_data.iloc[i, 0]} | {df_data.iloc[i, 1]}')

print('\n' + '='*70)
print('META FILE VERIFICATION')
print('='*70)

df_meta = pd.read_excel('output/latest/UKDMOGI_META_PART_1_20260113.xlsx', header=None)
print(f'\nTotal rows: {len(df_meta)}')
print(f'Total columns: {len(df_meta.columns)}')

print('\nHeaders:')
print(df_meta.iloc[0].tolist()[:8])

print('\nValues:')
print(df_meta.iloc[1].tolist()[:8])
