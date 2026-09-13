
import pandas as pd
import numpy as np

df = pd.read_csv('data/scans_2026-05-04.csv', dtype=str)
hubs = pd.read_csv('data/hubs.csv', dtype=str)

print('=== BASIC SHAPE ===')
print('Total rows:', len(df))
print('Columns:', list(df.columns))

print('\n=== 1. DUPLICATE SCAN_ID ===')
dup_scans = df['scan_id'].duplicated().sum()
print(f'Duplicate scan_id count: {dup_scans}')

print('\n=== 2. SCANNED_AT FORMATS ===')
fmt1_mask = df['scanned_at'].str.match(r'^\d{4}-\d{2}-\d{2}')
print(f'Standard format (YYYY-MM-DD...): {fmt1_mask.sum()}')
print(f'Non-standard format: {(~fmt1_mask).sum()} (about {(~fmt1_mask).mean()*100:.2f}%)')
print('Sample non-standard timestamps:', df.loc[~fmt1_mask, 'scanned_at'].head(3).tolist())

print('\n=== 3. WEIGHT_KG DEFECTS ===')
has_comma = df['weight_kg'].str.contains(',', na=False).sum()
is_blank = df['weight_kg'].isna().sum() | (df['weight_kg'].str.strip() == '').sum()
# Check impossible weights (e.g. negative or <= 0 after converting)
clean_wt = df['weight_kg'].str.replace(',', '.', regex=False)
numeric_wt = pd.to_numeric(clean_wt, errors='coerce')
impossible = (numeric_wt <= 0) | (numeric_wt > 100)
print(f'Comma as decimal: {has_comma}')
print(f'Blank/null weight: {is_blank}')
print(f'Impossible numeric weight (<= 0 or > 100kg): {impossible.sum()}')

print('\n=== 4. INVALID HUB_ID ===')
valid_hubs = set(hubs['hub_id'].dropna().unique())
invalid_hubs = (~df['hub_id'].isin(valid_hubs)).sum()
print(f'Hubs not in hubs.csv: {invalid_hubs}')
if invalid_hubs > 0:
    print('Sample invalid hubs:', df.loc[~df['hub_id'].isin(valid_hubs), 'hub_id'].unique().tolist())

print('\n=== 5. UNASSIGNED COURIER_ID ===')
blank_courier = df['courier_id'].isna().sum() | (df['courier_id'].str.strip() == '').sum()
print(f'Blank courier_id (real unassigned scans): {blank_courier}')

print('\n=== 6. SCAN_TYPE VALUES (CASE & DISTRIBUTION) ===')
print(df['scan_type'].value_counts(dropna=False))
