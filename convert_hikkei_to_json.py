import pandas as pd
import os
import re
import json     # JSON for generation

# ──────────────────────────────────
# 1. Text -> DataFrame
# ──────────────────────────────────
def process_questions(file_path):
    """Import without missing lines including case text"""
    with open(file_path, 'r', encoding='utf-16') as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    combined, current, in_q = [], [], False
    for ln in lines:
        if ln.startswith('問題'):
            if current:
                combined.append('\n'.join(current))  # Insurance
            current, in_q = [ln], True
            continue

        if in_q:                     # Inside question block
            current.append(ln)       # ★ Add case text and options
            if ln.startswith('解答'):
                combined.append('\n'.join(current))
                current, in_q = [], False
            continue

        combined.append(ln)          # Text outside questions

    if current:                      # Handle end of file
        combined.append('\n'.join(current))

    return pd.DataFrame(combined, columns=['Question'])

# ──────────────────────────────────
# 2. Various formatting functions (following original logic)
# ──────────────────────────────────
def generate_question_number(df):
    header = df.iloc[0, 0]
    if 'あん摩マッサージ指圧師試験' in header:
        exam_type = 'A'; m = re.search(r'第([０-９0-9]+)回', header)
    elif 'はり師・きゆう師試験' in header:
        exam_type = 'B'; m = re.search(r'第([０-９0-9]+)回', header)
    else:
        return df

    if not m:
        return df
    session = int(m.group(1).translate(str.maketrans('０１２３４５６７８９','0123456789')))
    prefix = f'{exam_type}{session:02}-'

    df['Serial Number'] = None
    for i, row in df.iterrows():
        if str(row['Question']).startswith('問題'):
            m2 = re.search(r'問題(\d+)', row['Question'])
            if m2:
                df.at[i, 'Serial Number'] = prefix + f'{int(m2.group(1)):03}'
    cols = ['Serial Number'] + [c for c in df.columns if c != 'Serial Number']
    return df[cols]

def replace_question_with_serial(df):
    mapping = {}
    for _, r in df.iterrows():
        if pd.notna(r['Serial Number']):
            m = re.search(r'問題([０-９0-9]+)', r['Question'])
            if m:
                mapping[f'{int(m.group(1).translate(str.maketrans("０１２３４５６７８９","0123456789"))):03}'] = r['Serial Number']

    def repl(m):
        return mapping.get(f'{int(m.group(1).translate(str.maketrans("０１２３４５６７８９","0123456789"))):03}', m.group(0))

    df['Question'] = df['Question'].apply(lambda x: re.sub(r'問題([０-９0-9]+)', repl, x))
    return df

def add_subject_to_questions_and_rearrange_columns(df):
    df['Subject'] = None
    current, drop_rows = None, []
    for i, r in df.iterrows():
        m = re.search(r'《([^》]+)》', r['Question'])
        if m:
            current = m.group(1)
            drop_rows.append(i)
        df.at[i, 'Subject'] = current
    df.drop(drop_rows, inplace=True)
    df.reset_index(drop=True, inplace=True)
    cols = ['Serial Number', 'Subject'] + [c for c in df.columns if c not in ('Serial Number','Subject')]
    return df[cols]

def store_case_details_next_to_questions(df):
    serial_re = re.compile(r'[AB]\d{2}-\d{3}')
    grouped_re = re.compile(r'([AB]\d{2})-(\d{3})(?:[、,](\d{1,3}))+')
    case_intro_re = re.compile(r'(次の.*症例|症例について)')
    df['Case Details'] = None

    def extract_serials(text):
        serials = set(serial_re.findall(text))
        for m in grouped_re.finditer(text):
            prefix = m.group(1)
            nums = re.findall(r'\d{1,3}', m.group(0))
            for num in nums:
                serials.add(f'{prefix}-{int(num):03}')
        return sorted(serials)

    for i, r in df.iterrows():
        text = r['Question']
        if re.match(r'^[AB]\d{2}-\d{3}', text):
            continue
        if '症例' not in text or not case_intro_re.search(text):
            continue
        sns = extract_serials(text)
        if sns:
            case_lines = [text]
            j = i + 1
            while j < len(df) and not re.match(r'^[AB]\d{2}-\d{3}', df.iloc[j]['Question']):
                case_lines.append(df.iloc[j]['Question'])
                j += 1
            combined = '\n'.join(case_lines)
            for sn in sns:
                df.loc[df['Serial Number']==sn, 'Case Details'] = combined
    df = df.dropna(subset=['Serial Number']).reset_index(drop=True)
    return df[['Serial Number','Subject','Case Details','Question']]

def remove_serial_number_from_questions(df):
    df['Question'] = df['Question'].apply(lambda x: re.sub(r'\b[AB]\d{2}-\d{3}\s+','',x))
    return df

def clean_subject_names(df):
    df['Subject'] = df['Subject'].str.strip().replace('衛生学／公衆衛生学','衛生学・公衆衛生学')
    return df

def add_exam_type_column(df):
    df['Exam Type'] = df['Serial Number'].str[0].map({'A':'あん摩マッサージ指圧師','B':'はり師・きゆう師'})
    return df[['Serial Number','Exam Type','Subject','Case Details','Question']]

def extract_exam_session_number(df):
    df['Exam Session'] = df['Serial Number'].str.extract(r'[AB](\d{2})-').astype(float).astype('Int64')
    return df[['Serial Number','Exam Type','Exam Session','Subject','Case Details','Question']]

# ──────────────────────────────────
# 3. DataFrame -> JS Conversion
# ──────────────────────────────────
def save_df_as_js(df, js_path, var_name='data'):
    json_str = df.to_json(orient='records', force_ascii=False, indent=2)
    js  = f"const {var_name} = {json_str};\n"
    js += f"window.{var_name} = {var_name};\n"   # Make accessible from other scripts
    with open(js_path,'w',encoding='utf-8') as f:
        f.write(js)

# ──────────────────────────────────
# 4. Main Processing
# ──────────────────────────────────
def process_all_files_in_directory(directory_path):
    txt_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.txt')]
    # Creating output directory at project-level output/convert
    base_dir = os.path.dirname(directory_path)
    output_dir = os.path.join(base_dir, 'output', 'convert')
    os.makedirs(output_dir, exist_ok=True)

    all_df = pd.DataFrame()

    print(f"Processing {len(txt_files)} files in {directory_path}...")

    for txt in txt_files:
        fp = os.path.join(directory_path, txt)
        # print(f"Processing {txt}...")
        try:
            df = (process_questions(fp)
                .pipe(generate_question_number)
                .pipe(replace_question_with_serial)
                .pipe(add_subject_to_questions_and_rearrange_columns)
                .pipe(store_case_details_next_to_questions)
                .pipe(remove_serial_number_from_questions)
                .pipe(clean_subject_names)
                .pipe(add_exam_type_column)
                .pipe(extract_exam_session_number))
            
            all_df = pd.concat([all_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error processing {txt}: {e}")

    # (1) Integrated Excel
    excel_path = os.path.join(output_dir, 'all_output_data.xlsx')
    all_df.to_excel(excel_path, index=False)
    print(f'Excel saved: {excel_path}')

    # (2) data.js
    js_path = os.path.join(output_dir, 'data.js')
    save_df_as_js(all_df, js_path)
    print(f'JS saved: {js_path}')

# ──────────────────────────────────
# 5. Execution
# ──────────────────────────────────
if __name__ == "__main__":
    # Assumes 'kokushitxt' is in the same directory as this script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.join(current_dir, 'kokushitxt')
    
    if os.path.exists(directory_path):
        process_all_files_in_directory(directory_path)
    else:
        print(f"Directory not found: {directory_path}")
