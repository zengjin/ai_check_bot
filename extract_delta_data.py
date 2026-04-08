import pandas as pd
import zipfile
from lxml import etree
import warnings
import os
import toml
from pathlib import Path

# --- 初期設定 ---
# openpyxlが古い形式のExcelファイルなどを読み込む際に出力する書式警告（スタイル無視など）を非表示にする
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def load_config(config_path="config.toml"):
    """
    TOML形式の設定ファイルを読み込む関数
    :param config_path: 設定ファイルのパス
    :return: 読み込んだ設定データの辞書
    """
    # ファイルの存在確認
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイル {config_path} が見つかりません")
    # TOMLファイルをパースして返す
    return toml.load(config_path)

def get_excel_shapes_lxml(file_path, target_labels):
    """
    lxmlを使用してExcel内部のXML構造を直接解析し、テキストボックス（図形）内のテキストを取得する
    ※通常のpandasやopenpyxlでは取得しにくい「描画オブジェクト」内のテキストを抽出する
    :param file_path: Excelファイルのパス (.xlsm/.xlsx)
    :param target_labels: 取得対象とする図形名（name属性）のリスト
    :return: {図形名: テキスト内容} の辞書
    """
    # 戻り値の初期化（見つからない場合は "Not Found"）
    results = {label: "Not Found" for label in target_labels}
    try:
        # Excelの実体はZipアーカイブなのでzipfileで開く
        with zipfile.ZipFile(file_path, 'r') as z:
            # 図形情報は xl/drawings/ フォルダ内のXMLに格納されている
            drawing_files = [f for f in z.namelist() if 'xl/drawings/drawing' in f]
            for d_file in drawing_files:
                xml_content = z.read(d_file)
                tree = etree.fromstring(xml_content)
                # XMLの名前空間を定義（SpreadsheetML Drawingなど）
                ns = {
                    'xdr': 'http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing',
                    'a': 'http://schemas.openxmlformats.org/drawingml/2006/main'
                }
                # 形状オブジェクト（xdr:sp）をすべて検索
                for shape in tree.xpath('//xdr:sp', namespaces=ns):
                    # 非ビジュアルプロパティから図形名を取得
                    nv_pr = shape.xpath('.//xdr:nvSpPr/xdr:cNvPr', namespaces=ns)
                    if nv_pr:
                        shape_name = nv_pr[0].get('name')
                        # 取得対象の図形名と一致した場合、その中のテキスト要素（a:t）を抽出
                        if shape_name in target_labels:
                            texts = shape.xpath('.//a:t', namespaces=ns)
                            # 分割されているテキストを結合して格納
                            results[shape_name] = "".join([t.text for t in texts if t.text])
    except Exception as e:
        print(f"エラー: {file_path} の解析に失敗しました: {e}")
    return results

def process_master_sheet(file_path, sheet_name, keys, head_row, data_row):
    """
    Excelシートを読み込み、データの型を正規化してクリーニングする
    :param file_path: Excelファイルのパス
    :param sheet_name: 読み込むシート名
    :param keys: 主キー（一意に特定するための列リスト）
    :param head_row: ヘッダー行番号（1開始）
    :param data_row: データ開始行番号（1開始）
    """
    # pandasのheader引数は0開始のため調整
    header_idx = head_row - 1
    
    # 【データ整合性のための対策1】
    # 読み込み時に型推論をオフにし、意図しない数値変換（"001" -> 1 など）を防ぐ
    df = pd.read_excel(
        file_path, 
        sheet_name=sheet_name, 
        header=header_idx, 
        dtype=str,           # すべての列を文字列として読み込む
        keep_default_na=False # 空白をNaNにせず、空文字""として扱う（数値計算の混入防止）
    )
    
    # ヘッダー行とデータ開始行の間に差がある場合、不要な行をスキップ
    skip_offset = data_row - head_row - 1
    if skip_offset > 0:
        df = df.iloc[skip_offset:].reset_index(drop=True)
    
    # 【データ整合性のための対策2】
    # ヘッダー名に含まれる改行コードを除去し、列名の一致精度を高める
    df.columns = [str(col).replace('\n', '').replace('\r', '') for col in df.columns]
    
    # 全データに対して再度文字列型を確定させ、前後の余計な空白を削除（トリミング）
    df = df.apply(lambda x: x.astype(str).str.strip())
    
    # データのクリーニング：
    # 1. すべての値が空の行を削除
    df = df.dropna(how='all')
    # 2. 主キーに欠損（空文字等）がある行を削除
    df = df.dropna(subset=keys)
        
    # 主キーでソートしてインデックスを再振り
    return df.sort_values(by=keys).reset_index(drop=True)

def compare_datasets(df1, df2, keys, ignore_cols):
    """
    2つのデータフレームを比較し、「追加」「削除」「修正」「未修正」を分類する
    :param df1: 比較元のデータ（旧）
    :param df2: 比較先のデータ（新）
    :param keys: 突き合わせに使用する主キー列
    :param ignore_cols: 値の修正比較から除外する列（更新日時など）
    """
    
    # 各データからキーの集合を抽出
    df1_keys = df1[keys].drop_duplicates()
    df2_keys = df2[keys].drop_duplicates()

    # --- 追加データの特定 ---
    # 新データ(df2)にあって旧データ(df1)にないキーを探す
    added_all = df2.merge(df1_keys, on=keys, how='left', indicator=True)
    added = added_all.query('_merge == "left_only"').drop('_merge', axis=1)
    
    # --- 削除データの特定 ---
    # 旧データ(df1)にあって新データ(df2)にないキーを探す
    deleted = df1.merge(df2_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

    # --- 修正・未修正の特定（共通キーの行を詳細比較） ---
    common_keys = df1_keys.merge(df2_keys, on=keys, how='inner')
    # 共通するキーを持つデータのみを取り出し、並び順を一致させる
    df1_c = df1.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)
    df2_c = df2.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)

    # 比較対象とする列（主キーと除外列以外）を決定
    compare_cols = [c for c in df1.columns if c not in keys and c not in ignore_cols]
    
    mod_idx, unmod_idx = [], []
    # 1行ずつ全列をループして差異があるか確認
    for i in range(len(df2_c)):
        row1, row2 = df1_c.iloc[i], df2_c.iloc[i]
        
        diff_found_in_row = False
        for c in compare_cols:
            val1 = row1[c]
            val2 = row2[c]
            
            # 空値などのゆらぎを考慮して文字列トリミング比較
            s1 = str(val1).strip() if pd.notna(val1) else ""
            s2 = str(val2).strip() if pd.notna(val2) else ""

            if s1 != s2:
                diff_found_in_row = True
                break # 1つでも差異があれば修正ありとみなす
        
        if diff_found_in_row:
            mod_idx.append(i)
        else:
            unmod_idx.append(i)

    # インデックスリストに基づきデータフレームを生成
    modified = df2_c.iloc[mod_idx] if mod_idx else pd.DataFrame(columns=df1.columns)
    unmodified = df2_c.iloc[unmod_idx] if unmod_idx else pd.DataFrame(columns=df1.columns)
    
    return added, deleted, modified, unmodified

def read_text_file(filename):
    """ファイルを読み込み、存在しない場合はエラーメッセージを返すユーティリティ"""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f: return f.read()
    return f"[{filename} が見つかりません]"

def extract_delta_data():
    # 1. 設定の読み込み
    conf = load_config()
    
    # 2. 設定パラメータの展開（TOML構造から取得）
    m_conf = conf['master_servicecode']
    sheet_name = m_conf['sheet_name']
    m_keys = m_conf['primary_keys']
    m_ignore = m_conf['ignore_cols']
    h_row = m_conf['head_row']
    d_row = m_conf['data_row']
    
    flags = conf['update_flag']
    p_files = conf['prompt_files']
    
    f1, f2 = "ServiceCode1.xlsm", "ServiceCode2.xlsm"
    labels = m_conf['excel_labels']
    
    # Excel内の特定図形テキスト（バージョン情報等）をコンソールに出力
    for f in [f1, f2]:
        print(f"\n--- {f} ---")
        for k, v in get_excel_shapes_lxml(f, labels).items(): print(f"{k}: {v}")

    # 3. 本体のデータ処理実行（読み込みと正規化）
    df1 = process_master_sheet(f1, sheet_name, m_keys, h_row, d_row)
    df2 = process_master_sheet(f2, sheet_name, m_keys, h_row, d_row)
    
    # データの比較実行
    added, deleted, modified, unmodified = compare_datasets(df1, df2, m_keys, m_ignore)

    print(f"\n統計: 追加({len(added)}) 削除({len(deleted)}) 修正({len(modified)}) 未修正({len(unmodified)})")

# 4. 更新フラグ（更新区分列）の整合性検証
    flag_col = flags['flag_col'] 
    
    # --- 「追加」データの検証 ---
    if not added.empty:
        invalid_added = added[added[flag_col] != flags['add']]
        if not invalid_added.empty:
            print(f"\n![警告] 追加データの{flag_col}が不正な行があります (期待値: {flags['add']})")
            print(invalid_added[m_keys + [flag_col]].to_markdown(index=False))
        else:
            print(f"OK: 追加データの{flag_col}はすべて '{flags['add']}' です")

    # --- 「修正」データの検証 ---
    if not modified.empty:
        invalid_modified = modified[modified[flag_col] != flags['update']]
        if not invalid_modified.empty:
            print(f"\n![警告] 修正データの{flag_col}が不正な行があります (期待値: {flags['update']})")
            print(invalid_modified[m_keys + [flag_col]].to_markdown(index=False))
        else:
            print(f"OK: 修正データの{flag_col}はすべて '{flags['update']}' です")

    # --- 「削除」データの検証 (追加部分) ---
    # 削除されたデータは旧ファイル(df1)側でのフラグ状態を確認する
    if not deleted.empty:
        # TOMLに 'delete' 定義があることを想定 (例: flags['delete'])
        delete_flag = flags.get('delete', '削除') 
        invalid_deleted = deleted[deleted[flag_col] != delete_flag]
        if not invalid_deleted.empty:
            print(f"\n![警告] 削除データの{flag_col}が不正な行があります (期待値: {delete_flag})")
            print(invalid_deleted[m_keys + [flag_col]].to_markdown(index=False))
        else:
            print(f"OK: 削除データの{flag_col}はすべて '{delete_flag}' です")

    # --- 「未修正」データの検証 (追加部分) ---
    if not unmodified.empty:
        # TOMLに 'unmodified' 定義があることを想定 (例: flags['unmodified'] 或いは空文字)
        unmod_flag = flags.get('unmodified', '') 
        invalid_unmodified = unmodified[unmodified[flag_col] != unmod_flag]
        if not invalid_unmodified.empty:
            print(f"\n![警告] 未修正データの{flag_col}が不正な行があります (期待値: '{unmod_flag}')")
            print(invalid_unmodified[m_keys + [flag_col]].to_markdown(index=False))
        else:
            print(f"OK: 未修正データの{flag_col}はすべて保持されています")

    # 5. LLM（生成AI）用プロンプトの構築
    # 各種テキストファイル（役割定義、形式指定など）を結合
    prompt_content = "\n".join([read_text_file(p_files[k]) for k in ['role', 'input_format', 'output_format', 'check_rules']])
    
    # 差分データをMarkdownテーブル形式に変換してプロンプトに埋め込む
    prompt = f"""{prompt_content}

### 追加データ (Markdown)
{added.to_markdown(index=False) if not added.empty else "なし"}

### 修正データ (Markdown)
{modified.to_markdown(index=False) if not modified.empty else "なし"}
"""

    # 結果をテキストファイルに出力
    with open('prompt.txt', 'w', encoding='utf-8') as f_out:
        f_out.write(prompt)
    
    print("\n--- prompt.txt を生成しました ---")

if __name__ == "__main__":
    extract_delta_data()