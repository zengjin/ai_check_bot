import pandas as pd
import warnings
import os
import toml
import time
import json
from google import genai
from pathlib import Path
from dotenv import load_dotenv

# --- 初期設定 ---
# Excel読み込み時のスタイル警告を非表示にする
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def load_config(config_path="config.toml"):
    """TOML形式の設定ファイルを読み込む"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイル {config_path} が見つかりません")
    return toml.load(config_path)

def process_master_sheet(file_path, sheet_name, keys, head_row, data_row):
    """Excelを読み込み、クレンジングを行う"""
    header_idx = head_row - 1
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_idx, dtype=str, keep_default_na=False)
    
    # 物理行番号の保持
    df['Excel_Row'] = df.index + header_idx + 2
    cols = ['Excel_Row'] + [c for c in df.columns if c != 'Excel_Row']
    df = df[cols]
    
    skip_offset = data_row - head_row - 1
    if skip_offset > 0:
        df = df.iloc[skip_offset:].reset_index(drop=True)
    
    df.columns = [str(col).replace('\n', '').replace('\r', '') for col in df.columns]
    data_cols = [c for c in df.columns if c != 'Excel_Row']
    df[data_cols] = df[data_cols].apply(lambda x: x.astype(str).str.strip())
    
    return df.dropna(how='all', subset=data_cols).dropna(subset=keys).reset_index(drop=True)

def compare_datasets(df1, df2, keys, ignore_cols):
    """新旧2つのデータを比較し、追加・削除・修正・未修正に分類する"""
    extended_ignore = ignore_cols + ['Excel_Row']
    df1_keys = df1[keys].drop_duplicates()
    df2_keys = df2[keys].drop_duplicates()

    # 追加と削除の抽出
    added = df2.merge(df1_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    deleted = df1.merge(df2_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

    # 共通キーの詳細比較
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
    """テキストファイルを読み込む"""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f: return f.read()
    return ""

def extract_delta_data(file_old, file_new, conf):
    """
    Excel比較を行い、追加・修正が統合されたデータフレームを返す。
    """
    m_conf = conf['master_servicecode']
    
    # データの読み込み
    df1 = process_master_sheet(file_old, m_conf['sheet_name'], m_conf['primary_keys'], m_conf['head_row'], m_conf['data_row'])
    df2 = process_master_sheet(file_new, m_conf['sheet_name'], m_conf['primary_keys'], m_conf['head_row'], m_conf['data_row'])
    
    # 差分抽出
    added, deleted, modified, unmodified = compare_datasets(df1, df2, m_conf['primary_keys'], m_conf['ignore_cols'])

    # 統計情報の表示
    print(f"\n--- 比較統計 ({file_new}) ---")
    print(f"追加: {len(added)}, 修正: {len(modified)}, 削除: {len(deleted)}, 未修正: {len(unmodified)}")

    if not deleted.empty:
        print(f"\n--- 削除されたデータ一覧 (旧ファイル物理行) ---")
        print(deleted.sort_values('Excel_Row')[['Excel_Row'] + m_conf['primary_keys']].to_markdown(index=False))

    # 更新区分の整合性チェック
    flags = conf['update_flag']
    flag_col = flags['flag_col'] 
    check_targets = [("追加", added, flags['add']), ("修正", modified, flags['update']), ("未修正", unmodified, flags['unmodified'])]

    for label, df, expected in check_targets:
        if not df.empty and expected:
            invalid = df[df[flag_col] != expected]
            if not invalid.empty:
                print(f"\n[警告] {label}データの {flag_col} 列が不正です (期待値: '{expected}')")
                print(invalid[['Excel_Row'] + m_conf['primary_keys'] + [flag_col]].to_markdown(index=False))

    # 追加と修正データを統合
    combined_delta = pd.concat([added, modified], ignore_index=True)
    if not combined_delta.empty:
        combined_delta = combined_delta.sort_values(by='Excel_Row').reset_index(drop=True)
    
    return combined_delta

def prompt_builder(file_old, file_new, conf):
    """
    プロンプトを構築し、ファイル(prompt_n.txt)に保存してリストで返す
    """
    df_delta = extract_delta_data(file_old, file_new, conf)
    if df_delta.empty: return []

    p_files = conf['prompt_files']
    prompt_base = "\n".join([read_text_file(p_files[k]) for k in ['role', 'input_format', 'output_format', 'check_rules']])
    
    prompts = []
    chunk_size = conf['llm_param']['chunk_size']
    total_pages = (len(df_delta) - 1) // chunk_size + 1

    for i in range(0, len(df_delta), chunk_size):
        page_num = i // chunk_size + 1
        chunk_md = df_delta.iloc[i : i + chunk_size].to_markdown(index=False)
        final_prompt = f"{prompt_base}\n\n### 対象データ (分割 {page_num}/{total_pages})\n{chunk_md}\n"
        
        # --- プロンプトをテキストファイルに出力 ---
        prompt_filename = f"prompt_{page_num}.txt"
        with open(prompt_filename, 'w', encoding='utf-8') as f:
            f.write(final_prompt)
        print(f"[保存] プロンプトを書き出しました: {prompt_filename}")
        
        prompts.append(final_prompt)
        
    return prompts

def llm_invoker(file_old, file_new):
    """
    最新の google.genai パッケージを使用した実行制御
    """
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    
    conf = load_config()
    llm_conf = conf['llm_param']
    llm_enabled = llm_conf.get('enabled', True)
    
    prompt_list = prompt_builder(file_old, file_new, conf)
    if not prompt_list:
        print("\n[通知] AI処理が必要な差分はありません。")
        return

    all_results = []

    if llm_enabled:
        if not api_key:
            raise ValueError(".env ファイルに GEMINI_API_KEY が設定されていません。")
            
        # --- 最新の google.genai クライアント初期化 ---
        client = genai.Client(api_key=api_key)
        model_id = llm_conf['model_id']
        
        print(f"\n>>> LLMモード: 有効 (最新SDKで実行中: {model_id})")

        for idx, prompt in enumerate(prompt_list, 1):
            print(f"--- Gemini呼び出し中 ({idx}/{len(prompt_list)}) ---")
            try:
                # generate_content の新しい呼び出し形式
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    config={
                        'response_mime_type': 'application/json'
                    }
                )
                
                # response.text または response.parsed を使用可能
                batch_data = json.loads(response.text)
                
                if isinstance(batch_data, list):
                    all_results.extend(batch_data)
                else:
                    all_results.append(batch_data)
                    
            except Exception as e:
                print(f"[エラー] チャンク {idx} で問題が発生しました: {e}")
            
            if idx < len(prompt_list):
                print("5秒待機...")
                time.sleep(5)
    else:
        print("\n>>> テストモード: 有効 (ローカルファイルから読み込みます)")
        for idx in range(1, len(prompt_list) + 1):
            test_filename = f"llm_result_{idx}.txt"
            content = read_text_file(test_filename)
            if content:
                try:
                    all_results.extend(json.loads(content))
                except Exception as e:
                    print(f"[エラー] {test_filename} のJSONデコードに失敗: {e}")

    # 結果の保存
    if all_results:
        output_df = pd.DataFrame(all_results)
        output_file = f"llm_result_{Path(file_new).stem}.xlsx"
        output_df.to_excel(output_file, index=False)
        print(f"\n[成功] 合計 {len(all_results)} 件を {output_file} に保存しました。")

if __name__ == "__main__":
    # 新旧Excelファイルを指定して実行
    llm_invoker("ServiceCode1.xlsm", "ServiceCode2.xlsm")