from jinja2 import Template
from datetime import datetime

# 1. プロンプトの定義（テンプレート）
prompt_template = """
今日は {{ DATE }} です。
この日付に基づいた、今日の運勢を教えてください。
"""

# 2. 注入する値の準備
today_str = datetime.now().strftime('%Y-%m-%d')

# 3. Jinja2でレンダリング
template = Template(prompt_template)
final_prompt = template.render(DATE=today_str)

print(final_prompt)