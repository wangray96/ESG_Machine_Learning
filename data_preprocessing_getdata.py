import pandas as pd

# 讀取csv檔案
financial_ratio = pd.read_csv('financial_ratio_monthly.csv')
fundamental_quarterly = pd.read_csv('fundamentals_quarterly.csv')

# 只保留gvkey相同的資料
common_gvkeys = set(financial_ratio['gvkey']).intersection(set(fundamental_quarterly['gvkey']))
financial_ratio = financial_ratio[financial_ratio['gvkey'].isin(common_gvkeys)]
fundamental_quarterly = fundamental_quarterly[fundamental_quarterly['gvkey'].isin(common_gvkeys)]

# 合併資料
merged_data = pd.merge(financial_ratio, fundamental_quarterly, how='left', left_on=['gvkey', 'public_date'], right_on=['gvkey', 'datadate'])

# 刪除指定的欄位
merged_data = merged_data.drop(columns=['costat', 'curcdq', 'datafmt', 'consol', 'popsrc', 'indfmt', 'fqtr','epsf12','epsfi12','epsfiq','epsfxq','epspi12','epspxq','epsx12','ancq','divyield'])


# 若合併後的資料集長度為0，則說明沒有任何匹配的資料
if len(merged_data) == 0:
    print("No matching data found!")
else:
    # 輸出合併後的資料到csv檔案
    merged_data.to_csv('merged_data.csv', index=False)
    print("Data has been merged and saved to 'merged_data.csv'")

# 讀取 csv 檔案
merger_data = pd.read_csv('merged_data.csv')
CRSP_Monthly_Stock_price = pd.read_csv('CRSP_Stock_price_Monthly.csv')


# 更改 PERMNO 欄位名稱為 permno
CRSP_Monthly_Stock_price.rename(columns={'PERMNO': 'permno'}, inplace=True)

# 保留符合 permno 的資料
common_permno = set(merger_data['permno']).intersection(set(CRSP_Monthly_Stock_price['permno']))
merger_data = merger_data[merger_data['permno'].isin(common_permno)]
CRSP_Monthly_Stock_price = CRSP_Monthly_Stock_price[CRSP_Monthly_Stock_price['permno'].isin(common_permno)]

# 將日期欄位轉換為 pandas datetime 格式，並創建新的年份和月份欄位
merger_data['public_date'] = pd.to_datetime(merger_data['public_date'])
merger_data['year'] = merger_data['public_date'].dt.year
merger_data['month'] = merger_data['public_date'].dt.month

CRSP_Monthly_Stock_price['date'] = pd.to_datetime(CRSP_Monthly_Stock_price['date'])
CRSP_Monthly_Stock_price['year'] = CRSP_Monthly_Stock_price['date'].dt.year
CRSP_Monthly_Stock_price['month'] = CRSP_Monthly_Stock_price['date'].dt.month

# 使用 permno, year, month 進行合併
merged_df = pd.merge(merger_data, CRSP_Monthly_Stock_price, on=['permno', 'year', 'month'], how='inner')

# 移除臨時新增的年份和月份欄位
merged_df.drop(columns=['year', 'month'], inplace=True)

# 刪除指定的欄位
columns_to_remove = ['DLSTCD', 'DLPDT', 'DLPRC','mkvaltq']
merged_df = merged_df.drop(columns=columns_to_remove)


# # 指定重新組織後的欄位順序
# cols = merged_df.columns.tolist()
# move_cols = ['TICKER_x', 'cusip_x', 'datadate', 'fyearq', 'tic', 'cusip_y', 'conm', 'datacqtr', 'datafqtr','permno', 'date', 'TICKER_y', 'COMNAM','CUSIP']
# start_index = cols.index('public_date') + 1
#
# # 移除需要移動的欄位，然後在指定位置插入
# for col in move_cols:
#     if col in cols:  # 加入這個條件檢查
#         cols.remove(col)
# insert_position = start_index
# for col in move_cols:
#     if col not in cols:  # 加入這個條件檢查
#         cols.insert(insert_position, col)
#         insert_position += 1
#
#
# # 重新組織DataFrame的欄位順序
# merged_df = merged_df[cols]

# 指定重新組織後的欄位順序
cols = merged_df.columns.tolist()
move_cols = ['TICKER_x', 'cusip_x', 'datadate', 'fyearq', 'tic', 'cusip_y', 'conm', 'datacqtr', 'datafqtr', 'permno', 'date', 'TICKER_y', 'COMNAM', 'CUSIP']

# 尋找 'public_date' 的位置
start_index = cols.index('public_date') + 1

# 移除需要移動的欄位
for col in move_cols:
    if col in cols:
        cols.remove(col)

# 在 'public_date' 之後插入指定的欄位
for col in move_cols:
    cols.insert(start_index, col)
    start_index += 1

# 重新組織DataFrame的欄位順序
merged_df = merged_df[cols]


# 將結果存為 csv 檔案
merged_df.to_csv('merged_result.csv', index=False)




