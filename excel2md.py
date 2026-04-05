import pandas as pd

# 指定 'ID_column' 这一列为字符串类型
df = pd.read_excel('ServiceCode2.xlsm', dtype={'ID_column': str})

# 转成 Markdown
markdown_text = df.to_markdown()
print(markdown_text)