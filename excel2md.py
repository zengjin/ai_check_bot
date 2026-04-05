import pandas as pd

df = pd.read_excel(
    'ServiceCode2.xlsm', 
    sheet_name='サービスコードマスタ（入力シート）', 
    header=1, 
    dtype=str,           # 全列文字列指定
    keep_default_na=False # 空白をNaNにせず空文字""にする(数値化を防ぐ)
)

df = df.iloc[4:].reset_index(drop=True)

# To Markdown
markdown_text = df.to_markdown()
print(markdown_text)