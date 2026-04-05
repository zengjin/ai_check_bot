import pandas as pd
import zipfile
from lxml import etree
import warnings
import os
import toml
from pathlib import Path

# openpyxlの書式警告を無視
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def load_config(config_path="config.toml"):
    """TOML設定ファイルを読み込む"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイル {config_path} が見つかりません")
    return toml.load(config_path)

def get_excel_shapes_lxml(file_path, target_labels):
    """lxmlを使用してExcel内部のXMLを解析し、テキストボックスの内容を取得する"""
    results = {label: "Not Found" for label in target_labels}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            drawing_files = [f for f in z.namelist() if 'xl/drawings/drawing' in f]
            for d_file in drawing_files:
                xml_content = z.read(d_file)
                tree = etree.fromstring(xml_content)
                ns = {
                    'xdr': 'http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing',
                    'a': 'http://schemas.openxmlformats.org/drawingml/2006/main'
                }
                for shape in tree.xpath('//xdr:sp', namespaces=ns):
                    nv_pr = shape.xpath('.//xdr:nvSpPr/xdr:cNvPr', namespaces=ns)
                    if nv_pr:
                        shape_name = nv_pr[0].get('name')
                        if shape_name in target_labels:
                            texts = shape.xpath('.//a:t', namespaces=ns)
                            results[shape_name] = "".join([t.text for t in texts if t.text])
    except Exception as e:
        print(f"エラー: {file_path} の解析に失敗しました: {e}")
    return results

def process_master_sheet(file_path, keys, head_row, data_row):
    """設定された行位置に基づきExcelを読み込み、前処理を行う"""
    sheet_name = "サービスコードマスタ（入力シート）"
    
    # pandasのheaderは0から始まるインデックス
    # 例：head_row=2 の場合、header=1 となる
    header_idx = head_row - 1
    
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_idx, dtype=str)
    
    # ヘッダー行からデータ行までのオフセットを計算
    # 例：ヘッダーが2行目、データが7行目から始まる場合、中間の4行をスキップする (7 - 2 - 1 = 4)
    skip_offset = data_row - head_row - 1
    if skip_offset > 0:
        df = df.iloc[skip_offset:].reset_index(drop=True)
    
    # データのクリーニング
    df = df.dropna(how='all')
    df.columns = [str(col).replace('\n', '').replace('\r', '') for col in df.columns]
    
    # 無効な主キー行を除外してフォーマットを整える
    df = df.dropna(subset=keys)
    for k in keys:
        df[k] = df[k].astype(str).str.strip()
        
    return df.sort_values(by=keys).reset_index(drop=True)

def compare_datasets(df1, df2, keys, ignore_cols):
    """比較ロジック：追加、削除、修正、未修正を識別する"""
    df1_keys = df1[keys].drop_duplicates()
    df2_keys = df2[keys].drop_duplicates()

    added = df2.merge(df1_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    deleted = df1.merge(df2_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

    common_keys = df1_keys.merge(df2_keys, on=keys, how='inner')
    df1_c = df1.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)
    df2_c = df2.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)

    compare_cols = [c for c in df1.columns if c not in keys and c not in ignore_cols]
    
    mod_idx, unmod_idx = [], []
    for i in range(len(df2_c)):
        row1, row2 = df1_c.iloc[i], df2_c.iloc[i]
        is_diff = any((str(row1[c]).strip() if pd.notna(row1[c]) else "") != 
                      (str(row2[c]).strip() if pd.notna(row2[c]) else "") for c in compare_cols)
        if is_diff: mod_idx.append(i)
        else: unmod_idx.append(i)

    modified = df2_c.iloc[mod_idx] if mod_idx else pd.DataFrame(columns=df1.columns)
    unmodified = df2_c.iloc[unmod_idx] if unmod_idx else pd.DataFrame(columns=df1.columns)
    return added, deleted, modified, unmodified

def read_text_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f: return f.read()
    return f"[{filename} が見つかりません]"

def main():
    # 1. 設定の読み込み
    conf = load_config()
    
    # 2. 設定パラメータの解析
    m_conf = conf['master_servicecode']
    m_keys = m_conf['primary_keys']
    m_ignore = m_conf['ignore_cols']
    h_row = m_conf['head_row']
    d_row = m_conf['data_row']
    
    flags = conf['update_flag']
    p_files = conf['prompt_files']
    # llm_cfg = conf['llm_param'] # API呼び出し用に予約
    
    f1, f2 = "ServiceCode1.xlsm", "ServiceCode2.xlsm"
    labels = m_conf['excel_labels']
    
    # Excelラベルの取得
    for f in [f1, f2]:
        print(f"\n--- {f} ---")
        for k, v in get_excel_shapes_lxml(f, labels).items(): print(f"{k}: {v}")

    # 3. データ処理の実行
    df1 = process_master_sheet(f1, m_keys, h_row, d_row)
    df2 = process_master_sheet(f2, m_keys, h_row, d_row)
    added, deleted, modified, unmodified = compare_datasets(df1, df2, m_keys, m_ignore)

    print(f"\n統計: 追加({len(added)}) 削除({len(deleted)}) 修正({len(modified)}) 未修正({len(unmodified)})")

    # 4. 更新フラグの検証
    if not added.empty and any(added["更新区分"] != flags['add']):
        print(f"![警告] 追加データの更新区分が不正です (期待値: {flags['add']})")
    if not modified.empty and any(modified["更新区分"] != flags['update']):
        print(f"![警告] 修正データの更新区分が不正です (期待値: {flags['update']})")

    # 5. LLMプロンプトの生成
    prompt_content = "\n".join([read_text_file(p_files[k]) for k in ['role', 'input_format', 'output_format', 'check_rules']])
    
    prompt = f"""{prompt_content}

### 追加データ (Markdown)
{added.to_markdown(index=False) if not added.empty else "なし"}

### 修正データ (Markdown)
{modified.to_markdown(index=False) if not modified.empty else "なし"}
"""

    with open('prompt.txt', 'w', encoding='utf-8') as f_out:
        f_out.write(prompt)
    
    print("\n--- prompt.txt を生成しました ---")

if __name__ == "__main__":
    main()