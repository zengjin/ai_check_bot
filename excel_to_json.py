import pandas as pd
import json

def excel_to_json(excel_file_path, json_file_path):
    """
    Excelファイルを読み込み、JSONファイルとして保存する関数
    """
    try:
        # 1. Excelファイルの読み込み
        # engine='openpyxl' を指定（.xlsx形式の場合）
        df = pd.read_excel(excel_file_path, engine='openpyxl')

        # 2. DataFrameを辞書のリスト形式に変換
        # orient='records' を指定することで、[{"列名": 値}, {"列名": 値}] の形式になります
        data = df.to_dict(orient='records')

        # 3. JSONファイルとして保存
        # force_ascii=False で日本語の文字化けを防ぎ、indent=4 で見やすく整形します
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"【完了】JSONファイルが作成されました: {json_file_path}")

    except FileNotFoundError:
        print(f"【エラー】指定されたファイルが見つかりません: {excel_file_path}")
    except Exception as e:
        print(f"【エラー】予期せぬエラーが発生しました: {e}")

# --- 実行セクション ---
if __name__ == "__main__":
    input_excel = 'llm_result_ServiceCode6.xlsx'
    output_json = 'reverse_result_ServiceCode6.json'

    excel_to_json(input_excel, output_json)