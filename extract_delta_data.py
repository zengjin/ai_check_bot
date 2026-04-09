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
    最新の google.genai パッケージを使用した実行制御。
    設定ファイルから列名を読み込み、中間処理として JSON を出力した後、Excel を生成する。
    """
    # 1. 環境変数と設定の読み込み
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    
    # 外部定義されていると想定される設定読み込み関数
    conf = load_config() 
    llm_conf = conf.get('llm_param', {})
    llm_enabled = llm_conf.get('enabled', True)
    
    # 出力フォーマットの設定（config.toml から列名を取得）
    output_conf = conf.get('llm_feedback', {})
    target_columns = output_conf.get('columns', [])
    
    # 2. プロンプトの構築
    prompt_list = prompt_builder(file_old, file_new, conf)
    
    # 出力ファイルパスの定義
    base_name = Path(file_new).stem
    output_json = f"llm_result_{base_name}.json"
    output_excel = f"llm_result_{base_name}.xlsx"
    
    all_results = []

    # AI処理が必要な差分がない場合の早期リターン
    if not prompt_list:
        print("\n[通知] AI処理が必要な差分はありません。ヘッダーのみのファイルを作成します。")
        # 空のJSONとExcelを作成
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4, ensure_ascii=False)
        pd.DataFrame(columns=target_columns).to_excel(output_excel, index=False)
        return

    # 3. LLM モードまたはテストモードの実行
    if llm_enabled:
        if not api_key:
            raise ValueError(".env ファイルに GEMINI_API_KEY が設定されていません。")
            
        # 最新の SDK クライアント初期化
        client = genai.Client(api_key=api_key)
        model_id = llm_conf.get('model_id', 'gemini-2.0-flash')
        
        print(f"\n>>> LLMモード: 有効 (モデル: {model_id})")

        for idx, prompt in enumerate(prompt_list, 1):
            print(f"--- Gemini呼び出し中 ({idx}/{len(prompt_list)}) ---")
            try:
                # JSONモードを強制してコンテンツ生成
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    config={'response_mime_type': 'application/json'}
                )
                
                batch_data = json.loads(response.text)
                
                # リスト形式か単一オブジェクトかを確認して追加
                if isinstance(batch_data, list):
                    all_results.extend(batch_data)
                else:
                    all_results.append(batch_data)
                    
            except Exception as e:
                print(f"[エラー] チャンク {idx} の処理中に例外が発生しました: {e}")
            
            # レート制限を考慮した待機
            if idx < len(prompt_list):
                print("5秒待機中...")
                time.sleep(5)
    else:
        # テストモード: ローカルファイルからデータを読み込み
        print("\n>>> テストモード: 有効 (ローカルファイルから読み込みます)")
        for idx in range(1, len(prompt_list) + 1):
            test_filename = f"llm_result_{idx}.txt"
            content = read_text_file(test_filename) # 外部定義の読み込み関数
            if content:
                try:
                    data = json.loads(content)
                    if isinstance(data, list):
                        all_results.extend(data)
                    else:
                        all_results.append(data)
                except Exception as e:
                    print(f"[エラー] {test_filename} の解析に失敗しました: {e}")

    # 4. JSON テキストファイルの生成（バックアップ用）
    print(f"\n>>> 中間データ(JSON)を保存中...")
    try:
        with open(output_json, 'w', encoding='utf-8') as f:
            # 読みやすさのためにインデントを追加し、日本語文字化け防止のため ensure_ascii=False
            json.dump(all_results, f, indent=4, ensure_ascii=False)
        print(f"[成功] JSONファイルが生成されました: {output_json}")
    except Exception as e:
        print(f"[警告] JSONファイルの保存に失敗しました: {e}")

    # 5. Excel ファイルの生成
    print(f"\n>>> Excelファイルを生成中...")
    if all_results:
        output_df = pd.DataFrame(all_results)

        # 【デバッグ用】JSONにある実際のキーと、設定ファイルの列名を表示
        print(f"[デバッグ] JSONの実際のキー: {list(output_df.columns)}")
        print(f"[デバッグ] 設定ファイルの列名: {target_columns}")

        # 設定された列順序に再配置（不要な列は削除、不足している列は空で追加）
        output_df = output_df.reindex(columns=target_columns)
        print(f"[情報] 合計 {len(all_results)} 件のデータを処理しました。")
    else:
        # 結果が空の場合、設定された列名のみを持つ DataFrame を作成
        output_df = pd.DataFrame(columns=target_columns)
        print("[通知] 処理結果が空です。空の表を作成します。")

    try:
        output_df.to_excel(output_excel, index=False)
        print(f"[成功] Excelファイルが保存されました: {output_excel}")
    except PermissionError:
        print(f"[致命的エラー] {output_excel} を保存できません。ファイルが開かれていないか確認してください。")

if __name__ == "__main__":
    # 新旧Excelファイルを指定して実行
#    llm_invoker("ServiceCode1.xlsm", "ServiceCode2.xlsm")
    llm_invoker("ServiceCode3.xlsm", "ServiceCode4.xlsm")
