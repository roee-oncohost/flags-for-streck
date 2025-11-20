    """_summary_

    Returns:
        _type_: _description_
    """

import os
import json
import pandas as pd


def read_text_file(path_to_text):
    """_summary_

    Args:
        path_to_text (_type_): _description_

    Returns:
        _type_: _description_
    """
    with open(path_to_text, 'r') as f:
        text_data = f.read()
    return text_data


def get_sections(text):
    """_summary_

    Args:
        text (_type_): _description_

    Returns:
        _type_: _description_
    """
    sections = text.strip().split('*\n')
    return sections


def analyze_section(section):
    """_summary_

    Args:
        section (_type_): _description_

    Returns:
        _type_: _description_
    """
    if not section.strip():
        return None
    lines = section.strip().split('\n')
    type_row = None
    header_row = None
    data_rows = []


def analyze_lines(lines):
    """_summary_

    Args:
        lines (_type_): _description_

    Returns:
        _type_: _description_
    """
    type_row = None
    header_row = None
    header_name = ''
    data_rows = []
    for line in lines:
        if line.startswith('TYPE\t'):
            type_row = line.split('\t')[1:] 
        elif line.startswith('FEPARAMS\t') or line.startswith('STATS\t') or line.startswith('FEATURES\t'):
            parts = line.split('\t')
            header_name = parts[0]
            header_row = parts[1:]
        elif line.startswith('DATA\t'):
                data_rows.append(line.split('\t')[1:])
    return type_row, header_row, header_name, data_rows


def create_df(type_row, header_row, data_rows, header_name, type_mappings):
    """
    Convert parsed rows to a dataframe
    
    Args:
        
    Returns:
        String in the original text format
    """
    
    type_mappings[header_name] = type_row
    df = pd.DataFrame(data_rows, columns=header_row)
    for col, dtype in zip(header_row, type_row):
        if col in df.columns:
            try:
                if dtype == 'integer':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif dtype == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif dtype == 'boolean':
                    df[col] = df[col].map({'0': False, '1': True, 'False': False, 'True': True})
                # 'text' stays as string
            except Exception as e:
                print(f"Warning: Could not convert column {col} to {dtype}: {e}")

    
    return df 


def dataframes_to_text(dataframes, type_mappings):
    """
    Convert parsed dataframes back to the original text format.
    
    Args:
        dataframes: Dictionary of dataframes (e.g., {'FEPARAMS': df1, 'STATS': df2})
        type_mappings: Dictionary mapping dataframe names to their type lists
                      (e.g., {'FEPARAMS': ['text', 'integer', ...], ...})
    
    Returns:
        String in the original text format
    """
    sections = []
    
    for name, df in dataframes.items():
        lines = []
        
        # Get the type mapping for this dataframe
        types = type_mappings.get(name, ['text'] * len(df.columns))
        
        # TYPE row
        type_line = 'TYPE\t' + '\t'.join(types)
        lines.append(type_line)
        
        # Header row
        header_line = name + '\t' + '\t'.join(df.columns)
        lines.append(header_line)
        
        # DATA rows
        for _, row in df.iterrows():
            # Convert values back to strings, handling special cases
            values = []
            for val in row:
                if pd.isna(val):
                    values.append('')
                elif isinstance(val, bool):
                    values.append('1' if val else '0')
                else:
                    values.append(str(val))
            
            data_line = 'DATA\t' + '\t'.join(values)
            lines.append(data_line)
        
        sections.append('\n'.join(lines))
    
    # Join sections with asterisk separator
    return '\n*\n'.join(sections) + '\n'

def make_text_file(file_name, text):
    with open(file_name, 'w') as f:
        f.write(text)



def read_workbook(file_path,
                tab_name=None,
                columns=['sample', 'well', 'clogged', 'low volume', 'name', 'lot number', 
                'sample type', 'slide', 'initial_chamber', 'subarray', "pdf_subarray", 'c2 aspiration',
                 'leak', 'sample notes', 'assay notes']):
    """_summary_

    Args:
        file_path (_type_): _description_
        tab_name (_type_, optional): _description_. Defaults to None.
        columns (list, optional): _description_. Defaults to ['sample', 'well', 'clogged', 'low volume', 'name', 'lot number', 'sample type', 'slide', 'initial_chamber', 'subarray', "pdf_subarray", 'c2 aspiration', 'leak', 'sample notes', 'assay notes'].

    Returns:
        _type_: _description_
    """
    if not tab_name:
        xls = pd.ExcelFile(file_path)
        tab_name = xls.sheet_names[3]
    workbook_df = pd.read_excel(file_path, sheet_name=tab_name, skiprows=7)
    workbook_df = workbook_df.iloc[:, :len(columns)]
    workbook_df.columns = columns
    workbook_df['slide'] = workbook_df['slide'].ffill()
    return workbook_df


def alter_scanner_files(text_path, workbook_path, streck_conversion_coefficients_path, streck_wells_list=['A3', 'B1']):
    """_summary_

    Args:
        text_path (_type_): _description_
        workbook_path (_type_): _description_
        streck_conversion_coefficients_path (_type_): _description_
        streck_wells_list (list, optional): _description_. Defaults to ['A3', 'B1'].
    """

    workbook_df = read_workbook(workbook_path)

    with open(streck_conversion_coefficients_path, 'r') as fp:
        streck_conversion_coefficients = json.load(fp)
    
    streck_workbook_df  = workbook_df[workbook_df['well'].isin(streck_wells_list)]
    files = [
        f for f in os.listdir(text_path)
        if os.path.isfile(os.path.join(text_path, f))
    ]
    text_files = [file for file in files if file.endswith('.txt')]
    streck_workbook_df['filename'] = streck_workbook_df.apply(match_file, axis=1, args=(text_files,))
    streck_files = streck_workbook_df['filename'].to_list()
    for streck_file in streck_files:
        file_path = os.path.join(text_path, streck_file)
        text = read_text_file(file_path)
        sections = get_sections(text)
        dataframes_dict = {}
        type_mappings = {}
        for i, section in enumerate(sections):
            lines = section.strip().split('\n')
            type_row, header_row, header_name, data_rows = analyze_lines(lines)
            df = create_df(type_row, header_row, data_rows, header_name, type_mappings)
            original_columns = df.columns
            if header_name=='FEATURES':
                probe_to_aptamer_dict = match_probe_aptamer(df)
                df['aptamer'] = df['ProbeName'].map(probe_to_aptamer_dict).fillna('other')
                df['conversion_coefficient'] = df['aptamer'].map(streck_conversion_coefficients)
                df['gProcessedSignal'] = df['gProcessedSignal'] * df['conversion_coefficient']
                df = df[original_columns]
                
                
                
            dataframes_dict[header_name] = df
        reconstituted_text = dataframes_to_text(dataframes_dict, type_mappings)
        make_text_file(file_path, reconstituted_text)    




def match_file(row, files):
    """_summary_ 

    Args:
        row (_type_): _description_
        files (_type_): _description_

    Returns:
        _type_: _description_
    """
    for f in files:
        if str(row['slide']) in f and f.endswith(row['pdf_subarray'] + '.txt'):
            return f
    return None

def match_probe_aptamer(df):
    """TODO"""
    probe_names = list(set(df['ProbeName'].to_list()))
    probe_names = [probe_name for probe_name in probe_names if probe_name.startswith('anti-')]
    probe_to_aptamer_dict = {probe_name: probe_name.split('anti-')[1].split('_')[0] for probe_name in probe_names}
    aptamer_to_probe_dict = {value: key for key, value in probe_to_aptamer_dict.items()}
    return probe_to_aptamer_dict


    








def test(dir_path):
    files = [file for file in os.listdir(dir_path) if file.endswith('.txt')]
    for file in files:
        file_path = os.path.join(dir_path, file)
        text = read_text_file(file_path)
        sections = get_sections(text)
        dataframes_dict = {}
        type_mappings = {}
        for i, section in enumerate(sections):
            lines = section.strip().split('\n')
            type_row, header_row, header_name, data_rows = analyze_lines(lines)
            df = create_df(type_row, header_row, data_rows, header_name, type_mappings)
            dataframes_dict[header_name] = df
        reconstituted_text = dataframes_to_text(dataframes_dict, type_mappings)
        make_text_file(file_path, reconstituted_text)
    
    

if __name__ == '__main__':
    alter_scanner_files('./data/OH2025_039/texts', './data/OH2025_039/OH2025_039 Workbook.xlsx', './data/streck_conversion_coefficients.json',['A1', 'C3'])
    # test('./data/texts')
    print('Done')
