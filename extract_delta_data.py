import pandas as pd
import zipfile
from lxml import etree
import warnings
import os
import toml
from pathlib import Path

# --- 初期設定 ---
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def load_config(config_path="config.toml"):
    """TOML形式の設定ファイルを読み込む"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイル {config_path} が見つかりません")
    return toml.load(config_path)

def get_excel_shapes_lxml(file_path, target_labels):
    """Excel内部のXMLを解析し、特定の図形テキストを取得する"""
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

def process_master_sheet(file_path, sheet_name, keys, head_row, data_row):
    """Excelを読み込み、元の行番号を先頭に保持した状態でクレンジングする"""
    header_idx = head_row - 1
    df = pd.read_excel(
        file_path, 
        sheet_name=sheet_name, 
        header=header_idx, 
        dtype=str, 
        keep_default_na=False
    )
    
    # 行番号の付与と列順の整理
    df['Excel_Row'] = df.index + header_idx + 2
    cols = ['Excel_Row'] + [c for c in df.columns if c != 'Excel_Row']
    df = df[cols]
    
    # データクレンジング
    skip_offset = data_row - head_row - 1
    if skip_offset > 0:
        df = df.iloc[skip_offset:].reset_index(drop=True)
    
    df.columns = [str(col).replace('\n', '').replace('\r', '') for col in df.columns]
    
    data_cols = [c for c in df.columns if c != 'Excel_Row']
    df[data_cols] = df[data_cols].apply(lambda x: x.astype(str).str.strip())
    
    df = df.dropna(how='all', subset=data_cols)
    df = df.dropna(subset=keys)
        
    return df.reset_index(drop=True)

def compare_datasets(df1, df2, keys, ignore_cols):
    """2つのデータを比較し、追加・削除・修正・未修正に分類する"""
    extended_ignore = ignore_cols + ['Excel_Row']
    
    df1_keys = df1[keys].drop_duplicates()
    df2_keys = df2[keys].drop_duplicates()

    added = df2.merge(df1_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    deleted = df1.merge(df2_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

    common_keys = df1_keys.merge(df2_keys, on=keys, how='inner')
    df1_c = df1.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)
    df2_c = df2.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)

    compare_cols = [c for c in df1.columns if c not in keys and c not in extended_ignore]
    
    mod_idx, unmod_idx = [], []
    for i in range(len(df2_c)):
        row1, row2 = df1_c.iloc[i], df2_c.iloc[i]
        diff_found = any(str(row1[c]).strip() != str(row2[c]).strip() for c in compare_cols)
        if diff_found: mod_idx.append(i)
        else: unmod_idx.append(i)

    modified = df2_c.iloc[mod_idx] if mod_idx else pd.DataFrame(columns=df2.columns)
    unmodified = df2_c.iloc[unmod_idx] if unmod_idx else pd.DataFrame(columns=df2.columns)
    
    return added, deleted, modified, unmodified

def read_text_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f: return f.read()
    return f"[{filename} が見つかりません]"

def extract_delta_data(file_old, file_new):
    """
    指定された新旧Excelファイルを比較し、プロンプト用テキストを返す
    """
    # 1. 設定の読み込み
    conf = load_config()
    m_conf = conf['master_servicecode']
    sheet_name, m_keys, m_ignore = m_conf['sheet_name'], m_conf['primary_keys'], m_conf['ignore_cols']
    h_row, d_row = m_conf['head_row'], m_conf['data_row']
    flags, p_files = conf['update_flag'], conf['prompt_files']
    
    # 2. データ読み込み
    df1 = process_master_sheet(file_old, sheet_name, m_keys, h_row, d_row)
    df2 = process_master_sheet(file_new, sheet_name, m_keys, h_row, d_row)
    
    # 3. 差分抽出
    added, deleted, modified, unmodified = compare_datasets(df1, df2, m_keys, m_ignore)

    # Excelの行番号順にソート
    added = added.sort_values(by='Excel_Row').reset_index(drop=True)
    deleted = deleted.sort_values(by='Excel_Row').reset_index(drop=True)
    modified = modified.sort_values(by='Excel_Row').reset_index(drop=True)
    unmodified = unmodified.sort_values(by='Excel_Row').reset_index(drop=True)

    print(f"\n[{file_new}] 統計: 追加({len(added)}) 削除({len(deleted)}) 修正({len(modified)}) 未修正({len(unmodified)})")

    # 4. 整合性検証
    flag_col = flags['flag_col'] 
    check_targets = [
        ("追加", added, flags['add']), 
        ("修正", modified, flags['update']), 
        ("未修正", unmodified, flags.get('unmodified', ''))
    ]

    for label, df, expected in check_targets:
        if not df.empty:
            invalid = df[df[flag_col] != expected]
            if not invalid.empty:
                print(f"![警告] {label}データの {flag_col} 不正 (期待: '{expected}')")
                print(invalid[['Excel_Row'] + m_keys + [flag_col]].to_markdown(index=False))

    if not deleted.empty:
        print(f"\n--- 削除されたデータ一覧 ({file_old} 物理行順) ---")
        print(deleted[['Excel_Row'] + m_keys + [flag_col]].to_markdown(index=False))

    # 5. LLM用プロンプトの構築
    prompt_parts = [read_text_file(p_files[k]) for k in ['role', 'input_format', 'output_format', 'check_rules']]
    prompt_base = "\n".join(prompt_parts)
    
    final_prompt = f"""{prompt_base}

### 追加データ (Markdown)
{added.to_markdown(index=False) if not added.empty else "該当なし"}

### 修正データ (Markdown)
{modified.to_markdown(index=False) if not modified.empty else "該当なし"}
"""
    return final_prompt

if __name__ == "__main__":
    # 使用例
    f1_name = "ServiceCode1.xlsm"
    f2_name = "ServiceCode2.xlsm"
    
    # 差分抽出とプロンプト取得
    generated_prompt = extract_delta_data(f1_name, f2_name)
    
    # 取得したプロンプトをファイルに保存したり、次の処理に回したりできる
    with open('prompt.txt', 'w', encoding='utf-8') as f_out:
        f_out.write(generated_prompt)
    
    print(f"\n--- 処理完了: prompt.txt を出力しました ---")