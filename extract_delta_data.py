import pandas as pd
import warnings
import os
import toml
from pathlib import Path

# --- 初期設定 ---
# Excel読み込み時のスタイル警告（openpyxl）を非表示にする
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def load_config(config_path="config.toml"):
    """TOML形式の設定ファイルを読み込む"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイル {config_path} が見つかりません")
    return toml.load(config_path)

def process_master_sheet(file_path, sheet_name, keys, head_row, data_row):
    """
    Excelを読み込み、クレンジングを行う。
    後の確認のため、元のExcel行番号を保持する。
    """
    header_idx = head_row - 1
    df = pd.read_excel(
        file_path, 
        sheet_name=sheet_name, 
        header=header_idx, 
        dtype=str, 
        keep_default_na=False
    )
    
    # Excelの物理行番号を計算 (インデックス + ヘッダー行オフセット + Excelの1始まり補正)
    df['Excel_Row'] = df.index + header_idx + 2
    
    # 列順を整理し、行番号を先頭に配置
    cols = ['Excel_Row'] + [c for c in df.columns if c != 'Excel_Row']
    df = df[cols]
    
    # ヘッダー行からデータ開始行までの不要な行を除去
    skip_offset = data_row - head_row - 1
    if skip_offset > 0:
        df = df.iloc[skip_offset:].reset_index(drop=True)
    
    # クレンジング：列名とデータ内の改行コード除去、および前後空白のトリム
    df.columns = [str(col).replace('\n', '').replace('\r', '') for col in df.columns]
    data_cols = [c for c in df.columns if c != 'Excel_Row']
    df[data_cols] = df[data_cols].apply(lambda x: x.astype(str).str.strip())
    
    # 全空行および主キーが欠損している行を除外
    df = df.dropna(how='all', subset=data_cols)
    df = df.dropna(subset=keys)
        
    return df.reset_index(drop=True)

def compare_datasets(df1, df2, keys, ignore_cols):
    """新旧2つのデータを比較し、追加・削除・修正・未修正に分類する"""
    extended_ignore = ignore_cols + ['Excel_Row']
    df1_keys = df1[keys].drop_duplicates()
    df2_keys = df2[keys].drop_duplicates()

    # 1. 追加(Added)と削除(Deleted)を抽出
    added = df2.merge(df1_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    deleted = df1.merge(df2_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

    # 2. 共通キーを持つ行の詳細比較
    common_keys = df1_keys.merge(df2_keys, on=keys, how='inner')
    df1_c = df1.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)
    df2_c = df2.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)

    # 比較対象となる列の特定
    compare_cols = [c for c in df1.columns if c not in keys and c not in extended_ignore]
    
    mod_idx, unmod_idx = [], []
    for i in range(len(df2_c)):
        row1, row2 = df1_c.iloc[i], df2_c.iloc[i]
        # いずれかの列に差分があれば「修正あり」と判定
        diff_found = any(str(row1[c]).strip() != str(row2[c]).strip() for c in compare_cols)
        if diff_found:
            mod_idx.append(i)
        else:
            unmod_idx.append(i)

    modified = df2_c.iloc[mod_idx] if mod_idx else pd.DataFrame(columns=df2.columns)
    unmodified = df2_c.iloc[unmod_idx] if unmod_idx else pd.DataFrame(columns=df2.columns)
    
    return added, deleted, modified, unmodified

def read_text_file(filename):
    """テキストファイルを読み込むユーティリティ関数"""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f: return f.read()
    return f"[{filename} が見つかりません]"

def extract_delta_data(file_old, file_new):
    """
    コアロジック：Excelを比較し、LLM用プロンプトのリストを生成する。
    差分がない場合は空のリストを返す。
    """
    # 1. 設定の読み込み
    conf = load_config()
    m_conf = conf['master_servicecode']
    chunk_size = conf['llm_param']['chunk_size']
    
    sheet_name = m_conf['sheet_name']
    m_keys = m_conf['primary_keys']
    m_ignore = m_conf['ignore_cols']
    h_row, d_row = m_conf['head_row'], m_conf['data_row']
    flags = conf['update_flag']
    p_files = conf['prompt_files']
    
    # 2. データの読み込みとクレンジング
    df1 = process_master_sheet(file_old, sheet_name, m_keys, h_row, d_row)
    df2 = process_master_sheet(file_new, sheet_name, m_keys, h_row, d_row)
    
    # 3. 差分抽出
    added, deleted, modified, unmodified = compare_datasets(df1, df2, m_keys, m_ignore)

    # 4. 統計情報の表示
    print(f"\n--- 比較統計 ({file_new}) ---")
    print(f"追加: {len(added)}, 修正: {len(modified)}, 削除: {len(deleted)}, 未修正: {len(unmodified)}")

    # 5. 更新区分(Flag)の整合性チェック
    flag_col = flags['flag_col'] 
    check_targets = [
        ("追加", added, flags['add']), 
        ("修正", modified, flags['update']), 
        ("未修正", unmodified, flags.get('unmodified', ''))
    ]

    for label, df, expected in check_targets:
        if not df.empty and expected:
            invalid = df[df[flag_col] != expected]
            if not invalid.empty:
                print(f"\n[警告] {label}データの {flag_col} 列が不正です (期待値: '{expected}')")
                print(invalid[['Excel_Row'] + m_keys + [flag_col]].to_markdown(index=False))
            else:
                print(f"OK: {label}データの {flag_col} チェック完了")

    # 削除されたデータのリストを表示 (プロンプトには含めない)
    if not deleted.empty:
        print(f"\n--- 削除されたデータ一覧 (旧ファイルの行番号順) ---")
        print(deleted.sort_values('Excel_Row')[['Excel_Row'] + m_keys].to_markdown(index=False))

    # 6. 追加と修正データを統合
    combined_delta = pd.concat([added, modified], ignore_index=True)
    
    # 【重要】追加・修正データが共に空の場合は、プロンプトを生成しない
    if combined_delta.empty:
        print("\n[通知] 追加および修正データが存在しないため、プロンプト生成をスキップします。")
        return []
    
    # Excelの物理行番号順にソート
    combined_delta = combined_delta.sort_values(by='Excel_Row').reset_index(drop=True)

    # 7. プロンプトテンプレートの構築
    prompt_parts = [read_text_file(p_files[k]) for k in ['role', 'input_format', 'output_format', 'check_rules']]
    prompt_base = "\n".join(prompt_parts)
    
    # 8. データのチャンク分割（切片化）
    prompts = []
    total_rows = len(combined_delta)
    
    for i in range(0, total_rows, chunk_size):
        chunk = combined_delta.iloc[i : i + chunk_size]
        chunk_md = chunk.to_markdown(index=False)
        
        page_num = i // chunk_size + 1
        total_pages = (total_rows - 1) // chunk_size + 1
        chunk_info = f"\n(データ分割: {page_num} / {total_pages})"
        
        final_prompt = f"{prompt_base}\n\n### 対象データ (Excel行番号順) {chunk_info}\n{chunk_md}\n"
        prompts.append(final_prompt)
        
    return prompts

if __name__ == "__main__":
    # 使用ファイルの設定
    old_file = "ServiceCode1.xlsm"
    new_file = "ServiceCode2.xlsm"
    
    # プロンプトリストの取得
    prompt_list = extract_delta_data(old_file, new_file)
    
    # 差分データがある場合のみファイルに出力
    if prompt_list:
        for idx, p in enumerate(prompt_list, 1):
            filename = f'prompt_{idx}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(p)
            print(f"\n[成功] プロンプトを保存しました: {filename} (文字数: {len(p)})")
    else:
        print("\n[終了] 処理が必要な差分データはありませんでした。")