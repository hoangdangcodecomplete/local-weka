import codecs
import csv
from datetime import datetime
import json
import re
import pandas as pd 
from common.common import get_cell_clean, get_clean_row_text, get_standard_dataframe, get_string_buffer_from_dataframe, get_test_code_clean
from common.labs.eurofins import convert_to_unique_format 

def extract_data_hill(): 
    all_lines = []
    count_empty = 0 
    file_path = r'D:\TnT\research\eurofins\src\csv\3049165-SSFC-1(20221220214938).csv' 
    with codecs.open(file_path, 'r', encoding='latin-1') as file: 
        for line in file:
            all_lines.append(line.strip())
    
    
    # Config structure of file
    paragraphs = {
            "title": [],
            "overview": [],
            "note": [],
            "records": []
        }
    paragraphs_map = ["title", "overview", "note", "records"]
    # Define the delimiter between Overview and Result 
    paragraphs_break = "--------------------------------------"
    # Map structure for paragraphs variable
    para = 0
    spec_key = ""
    for row in all_lines:
        if row.strip() == "":
            continue
        if paragraphs_break in row:
            para += 1
        else:
            row_clean, _numqt = get_clean_row_text(row, count_empty)
            if row_clean != '':
                if spec_key == '':
                    spec_key = _numqt
                paragraphs[paragraphs_map[para]].append(row_clean)
    # Start get overview from csv file
    item_overview = {}
    # Get title in overview information
    titles = []
    for t in paragraphs["title"]:
        titles.append(t) 
    item_overview["title"] = ''.join(titles)
    # Get information about the columns in the overview from the csv file 
    for ov in paragraphs["overview"]:
        _tmp = ov.split(':')
        if len(_tmp) == 1:
            item_overview[get_cell_clean(_tmp[0])] = ''
            continue
        if len(_tmp) > 2:
            _tmp = ov.split(':,')
        # print("t checking", _tmp)
        if ('Client Reference' in _tmp[0]) or 'Client Order Number' in _tmp[0]:
            item_overview[get_cell_clean(_tmp[0])] = get_cell_clean(_tmp[1]).split(".")[0]
        elif "File Creation Date" in _tmp[0] or "Date Registered" in _tmp[0]:
            item_overview[_tmp[0].strip().replace('"', '')] = convert_to_unique_format(get_cell_clean(_tmp[1]))
        else:
            item_overview[get_cell_clean(_tmp[0])] = get_cell_clean(_tmp[1])
    item_overview["cocid"] = '1600057'
    item_overview["note"] = ''.join(paragraphs["note"])
    job_lab_number = item_overview["Laboratory Job Number"]
    overview_df = pd.DataFrame([item_overview]) 
    cols = ['title', 'Laboratory Job Number','Date Registered','File Creation Date','Client Order Number','Client Reference', 'cocid']
    overview_df = overview_df[cols]
    overview_df['status']=''
    overview_df['fileurl']=f's3://S3_BUCKET_STORAGE_COC_FILES/1600057/abcde.csv'
    overview_df['fileid']= -1
    overview_df['filename']= 'abcde.csv'
    overview_df = overview_df.rename({'title': 'labname', 'Laboratory Job Number': 'labjobnumber','File Creation Date':'createdon','Client Order Number':'cocidmapping','Client Reference':'ttjobno','Date Registered':'dateregistered'}, axis=1)
    overview_df = overview_df.reindex(['fileid','filename','fileurl','createdon','labname','labjobnumber','cocidmapping','ttjobno','dateregistered','status', 'cocid'], axis=1)
    overview_df_body_data = get_string_buffer_from_dataframe(overview_df, False, csv.QUOTE_ALL)
    
    num_records = len(paragraphs["records"])
    if num_records > 0:
        idx_sample_type = []
        for ii in range(0,num_records):
            if "Sample Type" in paragraphs["records"][ii]:
                idx_sample_type.append(ii)
        results = []
        for idx_type in range(0, len(idx_sample_type)):
            sample_type = paragraphs["records"][idx_sample_type[idx_type]].replace('Sample Type:', "").replace('"', '').strip() if "Sample Type: " in paragraphs["records"][0] else ""
            records = []
            _key_sample_name = ",Sample Name:,"
            _key_lab_number = ",Lab Number:,"
            lab_name_lst = paragraphs["records"][idx_sample_type[idx_type] + 1].split(_key_sample_name)[1].split(',')
            lab_number_lst = paragraphs["records"][idx_sample_type[idx_type] + 2].split(_key_lab_number)[1].split(',')
            if idx_type == len(idx_sample_type) - 1:
                records = paragraphs["records"][idx_sample_type[idx_type]:]
            else:
                records = paragraphs["records"][idx_sample_type[idx_type]: idx_sample_type[idx_type + 1]]
            results = []
            index_unit = 1
            j = index_unit + 1
            skip_values = ["", "-"]
            for k in range(len(lab_number_lst)):
                test_category = ""
                for i in range(3, len(records)):
                    try:
                        item = {
                            "job_lab_number": job_lab_number,
                            "lab_number": lab_number_lst[k],
                            "sample_name": lab_name_lst[k]
                        }
                        _tmp = records[i].split(',')
                        test_analyze = ''
                        if len(_tmp) <= 2:
                            # Case "Dibenzo[a,h]anthracene",
                            test_category = ",".join(_tmp).replace('"', '')
                            continue
                        else:
                            # Case "Indeno(1,2,3-c,d)pyrene"
                            if '"' in _tmp[0]:
                                cates_part = [_tmp[0]]
                                for _i in range(1, len(_tmp)):
                                    cates_part.append(_tmp[_i])
                                    if '"' in _tmp[_i]:
                                        if _i == len(_tmp):
                                            test_category = ",".join(cates_part)
                                            continue
                                        else:
                                            abnomal_denta = _i
                                            test_analyze = get_test_code_clean(",".join(cates_part))
                                            
                        item['test_category'] = test_category
                        if test_analyze != '':
                            item['test_analyte'] = test_analyze
                            item['test_unit'] = _tmp[index_unit + abnomal_denta].strip()
                            item['test_value'] = _tmp[j + abnomal_denta].strip().replace('"', '')
                        else:
                            item['test_analyte'] = get_test_code_clean(_tmp[0])
                            item['test_unit'] = get_test_code_clean(_tmp[index_unit])
                            item['test_value'] = get_test_code_clean(_tmp[j])
                        # if item['test_value'] in skip_values:
                        #     continue
                        item["sample_type"] = sample_type
                        results.append(item)
                    except Exception as e:
                        print(f"error -- {e}")
                j += 1   
        records_df = pd.DataFrame(results)
    if num_records > 0:
        def result_numeric(row):
            try:
                if row['test_value'][0].isdigit() or row['test_value'][0]=='<' or row['test_value'][0]=='>':
                    val=str(row['test_value'])
                    val=re.findall("([-+]?\d*\.?\d+)", val)
                    val=float(val[0])
                    val=f"{val:.4f}"
                else:
                    val=None
                return val
            except:
                return None
        records_df['resultasnumeric'] = records_df.apply(result_numeric, axis=1)
        records_df['resultmathoperation']=records_df.apply(lambda row: row.test_value[0] if (len(row.test_value) > 0 and row.test_value[0] in (['<','>'])) else '', axis=1)
        if 'Asbestos Presence / Absence' in records_df['test_analyte'].values:
            records_df['asbestosdetected']=records_df.loc[records_df['test_analyte']=='Asbestos Presence / Absence'].apply(lambda row: 'Not Detected' if ('asbestos not' in (row.test_value).lower()) else 'Detected', axis=1)
        else:
            records_df['asbestosdetected'] = '' 
        records_df['labcategorycode']=''
        records_df['labtestcode']=''
        records_df['fileid']= -1
        records_df = records_df.drop('job_lab_number', axis=1)
        records_df = records_df.rename({'test_category': 'labcategoryname', 'test_analyte': 'labtestname','test_unit':'unit','test_value':'result','lab_number':'labno','sample_name':'labsamplename', 'sample_type': 'labsampletype'}, axis=1)
        records_df = records_df.reindex(['fileid','labsamplename','labcategoryname','labcategorycode','labtestname','labtestcode','unit','result','resultasnumeric','resultmathoperation','asbestosdetected', 'labsampletype'], axis=1)
        records_df_body_data = get_string_buffer_from_dataframe(records_df, False, csv.QUOTE_ALL)
        
    response = {
        "header": json.loads(overview_df.to_json(orient="records")),
        "details": json.loads(records_df.to_json(orient="records"))
    }
    
    print(f'response: {response}')