import pandas as pd
import random

# 讀取CSV檔案
df = pd.read_csv('sorted_result.csv')

# 將public_date轉換為日期格式
df['public_date'] = pd.to_datetime(df['public_date'])

# 只保留1998-01之後的資料
df = df[df['public_date'] >= '1998-01-01']

# 定義兩組不同的刪除規則
columns_80 = ['epspiq', 'revtq']
columns_66 = ['pe_exi', 'pe_inc', 'gpm', 'roe', 'ptb', 'PRC', 'ALTPRC']

columns_to_fill = columns_80 + columns_66

# 依照gvkey拆開成不同dataframe，相同gvkey在同個dataframe
dfs_by_gvkey = [group for _, group in df.groupby('gvkey')]

# 初始化用於記錄保留和刪除的gvkey的集合
gvkeys_deleted = set()
gvkeys_kept = set()

# 檢查每個dataframe中的指定columns
dfs_to_keep = []
for sub_df in dfs_by_gvkey:
    drop_df = False

    # 檢查第一組刪除規則：缺失值達80%以上
    for col in columns_80:
        if sub_df[col].isna().mean() > 0.80:
            drop_df = True
            break

    # 檢查第二組刪除規則：缺失值達66%以上
    if not drop_df:
        for col in columns_66:
            if sub_df[col].isna().mean() > 0.70:
                drop_df = True
                break

    if drop_df:
        gvkeys_deleted.add(sub_df['gvkey'].iloc[0])
    else:
        gvkeys_kept.add(sub_df['gvkey'].iloc[0])
        dfs_to_keep.append(sub_df)


# 對於保留的dataframes執行缺失值補齊
def fill_missing_values(sub_df):
    for col in columns_to_fill:
        if col in ['epspiq', 'revtq']:
            # 依照前值補齊
            sub_df[col].fillna(method='ffill', inplace=True)
            # 沒有前值才依照後值補齊
            sub_df[col].fillna(method='bfill', inplace=True)
        else:
            # 上下平均補齊
            sub_df[col].interpolate(method='linear', inplace=True, limit_direction='both')

            # 首位缺失值補齊
            if pd.isna(sub_df[col].iloc[0]):
                sub_df[col].iloc[0] = sub_df[col].dropna().iloc[0]

            # 末位缺失值補齊
            if pd.isna(sub_df[col].iloc[-1]):
                sub_df[col].iloc[-1] = sub_df[col].dropna().iloc[-1]
    return sub_df


filled_dfs = [fill_missing_values(group) for group in dfs_to_keep]

# 將補齊缺失值後的dataframes上下合併在一起
final_df = pd.concat(filled_dfs, ignore_index=True)

# 輸出合併後的DataFrame
final_df.to_csv('processed_filled_result.csv', index=False)

# 輸出結果
print(f"Number of gvkeys deleted: {len(gvkeys_deleted)}")
print(f"Number of gvkeys kept: {len(gvkeys_kept)}")

# 隨機列出5個被刪除的gvkey
list_of_deleted_gvkeys = list(gvkeys_deleted)
if len(list_of_deleted_gvkeys) >= 5:
    random_deleted_gvkeys = random.sample(list_of_deleted_gvkeys, 5)
    print(f"Randomly selected 5 deleted gvkeys: {', '.join(map(str, random_deleted_gvkeys))}")
else:
    print(f"All deleted gvkeys: {', '.join(map(str, list_of_deleted_gvkeys))}")
