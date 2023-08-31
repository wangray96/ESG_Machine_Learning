import pandas as pd

# 讀取 CSV 檔
df = pd.read_csv('merged_result.csv')

# 用 gvkey 切分資料
grouped = [group for _, group in df.groupby('gvkey')]

# 依照 public_date 排序每個 gvkey 的 DataFrame
sorted_grouped = [group.sort_values('public_date') for group in grouped]

# 將所有的 DataFrame 上下合併
final_df = pd.concat(sorted_grouped)

# 輸出到一個新的 CSV 檔
final_df.to_csv('sorted_result.csv', index=False)
