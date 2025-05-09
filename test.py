import pandas as pd

df = pd.read_excel('./data/分省年度数据-规模以上工业营业收入.xls', )

df_new = df.set_index("地区").T.reset_index()
df_new.columns = ["时间"] + df["地区"].tolist()

df_new.to_excel('./分省年度数据-规模以上工业营业收入.xlsx', index=False)
