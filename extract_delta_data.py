import pandas as pd
import warnings
import os
import toml
from pathlib import Path

# --- 初期設定 ---
# 忽略 openpyxl 读取 Excel 时的样式警告
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def load_config(config_path="config.toml"):
    """读取 TOML 格式的配置文件"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイル {config_path} が見つかりません")
    return toml.load(config_path)

def process_master_sheet(file_path, sheet_name, keys, head_row, data_row):
    """
    读取 Excel 并进行数据清洗。
    保留原始 Excel 行号以便后续追踪。
    """
    header_idx = head_row - 1
    df = pd.read_excel(
        file_path, 
        sheet_name=sheet_name, 
        header=header_idx, 
        dtype=str, 
        keep_default_na=False
    )
    
    # 计算 Excel 实际行号 (索引 + 表头行偏移 + Excel从1开始计数的修正)
    df['Excel_Row'] = df.index + header_idx + 2
    
    # 调整列顺序，将行号置于首列
    cols = ['Excel_Row'] + [c for c in df.columns if c != 'Excel_Row']
    df = df[cols]
    
    # 过滤掉表头到数据起始行之间的冗余行
    skip_offset = data_row - head_row - 1
    if skip_offset > 0:
        df = df.iloc[skip_offset:].reset_index(drop=True)
    
    # 清洗：去除列名和内容中的换行符及首尾空格
    df.columns = [str(col).replace('\n', '').replace('\r', '') for col in df.columns]
    data_cols = [c for c in df.columns if c != 'Excel_Row']
    df[data_cols] = df[data_cols].apply(lambda x: x.astype(str).str.strip())
    
    # 删除全空行和主键缺失行
    df = df.dropna(how='all', subset=data_cols)
    df = df.dropna(subset=keys)
        
    return df.reset_index(drop=True)

def compare_datasets(df1, df2, keys, ignore_cols):
    """对比新旧两个数据集，分类为：新增、删除、修改、未修改"""
    extended_ignore = ignore_cols + ['Excel_Row']
    df1_keys = df1[keys].drop_duplicates()
    df2_keys = df2[keys].drop_duplicates()

    # 1. 提取新增 (Added) 和 删除 (Deleted)
    added = df2.merge(df1_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    deleted = df1.merge(df2_keys, on=keys, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

    # 2. 提取共有主键的行进行详细比较
    common_keys = df1_keys.merge(df2_keys, on=keys, how='inner')
    df1_c = df1.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)
    df2_c = df2.merge(common_keys, on=keys, how='inner').sort_values(keys).reset_index(drop=True)

    # 确定需要对比内容的列
    compare_cols = [c for c in df1.columns if c not in keys and c not in extended_ignore]
    
    mod_idx, unmod_idx = [], []
    for i in range(len(df2_c)):
        row1, row2 = df1_c.iloc[i], df2_c.iloc[i]
        # 只要有一个字段不同，即判定为已修改
        diff_found = any(str(row1[c]).strip() != str(row2[c]).strip() for c in compare_cols)
        if diff_found:
            mod_idx.append(i)
        else:
            unmod_idx.append(i)

    modified = df2_c.iloc[mod_idx] if mod_idx else pd.DataFrame(columns=df2.columns)
    unmodified = df2_c.iloc[unmod_idx] if unmod_idx else pd.DataFrame(columns=df2.columns)
    
    return added, deleted, modified, unmodified

def read_text_file(filename):
    """读取文本文件工具函数"""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f: return f.read()
    return f"[{filename} 未找到]"

def extract_delta_data(file_old, file_new):
    """
    核心逻辑：对比 Excel 文件并生成切片后的提示词列表。
    如果没有差异数据，则返回空列表。
    """
    # 1. 配置加载
    conf = load_config()
    m_conf = conf['master_servicecode']
    chunk_size = conf['llm_param']['chunk_size']
    
    sheet_name = m_conf['sheet_name']
    m_keys = m_conf['primary_keys']
    m_ignore = m_conf['ignore_cols']
    h_row, d_row = m_conf['head_row'], m_conf['data_row']
    flags = conf['update_flag']
    p_files = conf['prompt_files']
    
    # 2. 数据读取与清洗
    df1 = process_master_sheet(file_old, sheet_name, m_keys, h_row, d_row)
    df2 = process_master_sheet(file_new, sheet_name, m_keys, h_row, d_row)
    
    # 3. 差分计算
    added, deleted, modified, unmodified = compare_datasets(df1, df2, m_keys, m_ignore)

    # 4. 打印统计报告
    print(f"\n--- 统计结果 ({file_new}) ---")
    print(f"新增: {len(added)}, 修正: {len(modified)}, 删除: {len(deleted)}, 未变化: {len(unmodified)}")

    # 5. 业务逻辑校验 (Flag 校验)
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
                print(f"\n[警告] {label}数据中的 {flag_col} 字段与实际差异不符 (期待值: '{expected}')")
                print(invalid[['Excel_Row'] + m_keys + [flag_col]].to_markdown(index=False))
            else:
                print(f"OK: {label}数据的 {flag_col} 状态校验通过")

    # 显示删除列表 (仅控制台打印，不进入 LLM 提示词)
    if not deleted.empty:
        print(f"\n--- 已删除数据 (参考自旧文件行号) ---")
        print(deleted.sort_values('Excel_Row')[['Excel_Row'] + m_keys].to_markdown(index=False))

    # 6. 合并新增和修改的数据
    combined_delta = pd.concat([added, modified], ignore_index=True)
    
    # 【逻辑修改点】如果没有需要 LLM 处理的增量数据，直接返回空列表
    if combined_delta.empty:
        print("\n[通知] 没有发现新增或修正的数据，将不生成提示词文件。")
        return []
    
    # 按原始物理行号排序
    combined_delta = combined_delta.sort_values(by='Excel_Row').reset_index(drop=True)

    # 7. 准备提示词模板
    prompt_parts = [read_text_file(p_files[k]) for k in ['role', 'input_format', 'output_format', 'check_rules']]
    prompt_base = "\n".join(prompt_parts)
    
    # 8. 差分数据切片处理
    prompts = []
    total_rows = len(combined_delta)
    
    for i in range(0, total_rows, chunk_size):
        chunk = combined_delta.iloc[i : i + chunk_size]
        chunk_md = chunk.to_markdown(index=False)
        
        page_num = i // chunk_size + 1
        total_pages = (total_rows - 1) // chunk_size + 1
        chunk_info = f"\n(数据分片: {page_num} / {total_pages})"
        
        final_prompt = f"{prompt_base}\n\n### 待处理差分数据 (Excel行号顺序) {chunk_info}\n{chunk_md}\n"
        prompts.append(final_prompt)
        
    return prompts

if __name__ == "__main__":
    # 配置新旧文件名
    old_file = "ServiceCode1.xlsm"
    new_file = "ServiceCode2.xlsm"
    
    # 获取提示词列表
    prompt_list = extract_delta_data(old_file, new_file)
    
    # 【逻辑修改点】只有当有提示词生成时，才执行文件写入
    if prompt_list:
        for idx, p in enumerate(prompt_list, 1):
            filename = f'prompt_{idx}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(p)
            print(f"\n[成功] 已保存提示词文件: {filename} (字符数: {len(p)})")
    else:
        print("\n[结束] 程序运行完毕，无任何输出文件。")