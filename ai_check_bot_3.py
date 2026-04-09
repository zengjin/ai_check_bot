from extract_delta_data import llm_invoker
from feedback import mark_and_update_excel_errors
import pandas as pd

llm_invoker("ServiceCode5.xlsm", "ServiceCode6.xlsm")

# 外部のエラーリストファイルを指定
error_list_file = "llm_result_ServiceCode6.xlsx"

try:
    # エラーリストを読み込み、辞書形式のリストに変換
    error_df = pd.read_excel(error_list_file, dtype=str)
    error_data_json = error_df.to_dict(orient='records')

    # 主キーとして使用するカラム名
    pks = ["NTTグループ会社コード", "サービスコード（値）"]

    # 実行
    mark_and_update_excel_errors("ServiceCode5.xlsm", "ServiceCode6_Fixed.xlsx", error_data_json, pks)
except Exception as e:
    print(f"エラーリストの読み込みに失敗しました: {e}")

