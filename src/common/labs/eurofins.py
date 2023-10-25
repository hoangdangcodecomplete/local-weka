import codecs
import csv
from datetime import datetime
from io import StringIO
import json
import re
import pandas as pd 
from common.common import get_cell_clean, get_standard_dataframe, get_string_buffer_from_dataframe 

asbstos_lab_names= ["Asbestos in Soils (AS 4964-2004)",
                    "Materials Identified", 
                    "Fibres Identified and estimated Asbestos Content (%)",
                    "ACM % asbestos (weighted average)", "ACM in Soil (as asbestos)",
                    "AF % asbestos (weighted average)", "Combined AF+FA",
                    "Asbestos Sample Mass/Dimensions", "Asbestos Reported Result"
                    ]

def convert_to_unique_format(date_str):
    """
    Definition:
        - Convert date format to a specific format

    Args:
        - date_str(string): Date time value 

    Returns:
        - str: output string format datetime
    """
    date_patterns = ["%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M"]
    for pattern in date_patterns:
        try:
            return datetime.strptime(date_str, pattern).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
    return date_str

def extract_eurofins_data():
    try: 
        csv_buffer = StringIO()
        paragraphs = { 
                        "header": [], 
                        "records": []
                    }
        n = 6
        file_path = r'D:\TnT\research\eurofins\src\csv\1002810_data_test.csv' 
        with codecs.open(file_path, 'r', encoding='latin-1') as file:
            for line in file: 
                csv_buffer.write(line)
        
        
        pattern = r'"([^"]+)"'
        
        def replace_comma_and_newline(match):
            text = match.group(1)
            text = text.replace(',', '').replace('\n', '')
            return '"' + text + '"'
    
        csv_buffer.seek(0)
        csv_content = csv_buffer.getvalue()

        # # Thay thế tất cả dấu phẩy bằng chuỗi rỗng
        # replacement = lambda match: match.group(1).replace(',', '')
        # csv_content = re.sub(pattern, replacement, csv_content)

        # # Loại bỏ tất cả các dấu xuống dòng 
        # replacement = lambda match: match.group(1).replace('\\n', '')
        # csv_content = re.sub(pattern, replacement, csv_content) 
        
        csv_content = re.sub(pattern, replace_comma_and_newline, csv_content)

        csv_buffer.seek(0)
        csv_buffer.truncate()
        csv_buffer.write(csv_content)
        csv_buffer.seek(0)
         
        with open("output.csv", "w") as file:
            file.write(csv_buffer.getvalue())
        
        i = 0
        for line in csv_buffer:
            if i < 6:
                    paragraphs['header'].append(line)
            else: 
                paragraphs['records'].append(line)
            i = i + 1 
        
        results, asbstos_results = correct_asbstos_logic(paragraphs['records'])
        paragraphs['records'] = results
        
        # get overview_df and job_lab_number
        overview_df, job_lab_number = extract_data_header(paragraphs=paragraphs, filename='QUAN.csv') 
        
        with open("output.csv", "w") as file: 
            for item in paragraphs['records']:
                file.write(item + '\n')
        
        # get record data
        records_df = extract_data_records(paragraphs=paragraphs, job_lab_number=job_lab_number, asbstos_records=asbstos_results)
        
        response = {
            "header": json.loads(overview_df.to_json(orient="records")),
            "details": json.loads(records_df.to_json(orient="records"))
        }
        
        print(f'response: {response}')
        return response

    except Exception as ex:
        print(f'Error method extract_eurofins_data: {ex}')

def extract_data_header(paragraphs, filename):
    try: 
        item_overview = {}
        for ov in paragraphs["header"]:
            _tmp = ov.split('":,"')
            if len(_tmp) == 1:
                _tmp = ov.split(':')
            if len(_tmp) ==  1 and 'Client Reference' not in _tmp[0]:
                continue
            if 'Client Order Number' in _tmp[0]:
                item_overview[get_cell_clean(_tmp[0])] = get_cell_clean(_tmp[1]).split(".")[0]
            elif "File Creation Date" in _tmp[0] or "Date Registered" in _tmp[0]:
                item_overview[_tmp[0].strip().replace('"', '')] = convert_to_unique_format(get_cell_clean(_tmp[1]))
            elif('Client Reference' in _tmp[0]):
                item_overview['Client Reference'] = get_cell_clean(_tmp[0].split(",")[1])
            else:
                item_overview[get_cell_clean(_tmp[0])] = get_cell_clean(_tmp[1]) 
            
        # set default value
        item_overview["title"] = 'Eurofins Laboratory'
        item_overview["File Creation Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item_overview["Date Registered"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item_overview["note"] = ''
        
        # replace value cocid
        item_overview["Client Order Number"] = item_overview["Client Order Number"].replace('COC','')
        job_lab_number = item_overview["Laboratory Job Number"]
        
        item_overview["cocid"] = item_overview["Client Order Number"]
        overview_df = pd.DataFrame([item_overview])
        overview_df = overview_df.replace({'"': ''}, regex=True)
        overview_df = get_standard_dataframe(overview_df)
        cols = ['title', 'Laboratory Job Number','Date Registered','File Creation Date','Client Order Number','Client Reference', 'cocid']
        overview_df = overview_df[cols]
        overview_df['status']=''
        overview_df['fileurl']=f's3://S3_BUCKET_STORAGE_COC_FILES/' + item_overview["Client Order Number"] + "/" + filename
        overview_df['fileid']= -1
        overview_df['filename']=filename
        overview_df = overview_df.rename({'title': 'labname', 'Laboratory Job Number': 'labjobnumber','File Creation Date':'createdon','Client Order Number':'cocidmapping','Client Reference':'ttjobno','Date Registered':'dateregistered'}, axis=1)
        overview_df = overview_df.reindex(['fileid','filename','fileurl','createdon','labname','labjobnumber','cocidmapping','ttjobno','dateregistered','status', 'cocid'], axis=1)
        #overview_df_body_data = get_string_buffer_from_dataframe(overview_df, False, csv.QUOTE_ALL)
        
        return overview_df, job_lab_number
         
    except Exception as ex:
        print(f'Error method extract_data_header: {ex}') 

def check_category(row):
    try:
        row = row.replace("\r\n", "")
        row = row.split(',')
        if all(x.strip() == '' for x in row[0:]):
            return "Empty row"
        
        if row[0] and all(x.strip() == '' for x in row[1:]):
            return "Category"
        else:
            return "Normal Row"
    except Exception as ex:
        print(f'Error method check_category: {ex}')
    
def extract_data_records(paragraphs, job_lab_number, asbstos_records): 
    try:
        num_records = len(paragraphs["records"])
        if num_records > 0:
            idx_sample_type = []
            for ii in range(0,num_records):
                if "Sample Type" in paragraphs["records"][ii]:
                    idx_sample_type.append(ii)
            results = []
            for idx_type in range(0, len(idx_sample_type)):
                sample_type = paragraphs["records"][idx_sample_type[idx_type]].replace('Sample Type:', "").replace(',', '').strip() if "Sample Type: " in paragraphs["records"][5].replace(',','').strip() else ""
                records = []
                lab_name_lst = paragraphs["records"][idx_sample_type[idx_type] - 4].split(',')
                lab_number_lst = paragraphs["records"][idx_sample_type[idx_type] - 5].split(',')
                skip_indices = []
                index_unit = -1
                for i in range(len(lab_number_lst)):
                    if lab_number_lst[i] == "" or "Lab sample ID" in lab_number_lst[i]:
                        if "Lab sample ID" in lab_number_lst[i]:
                            index_unit = i + 1
                        skip_indices.append(i)
                skip_indices = sorted(skip_indices, reverse=True)
                for idx in skip_indices:
                    if idx < len(lab_number_lst):
                        lab_number_lst.pop(idx)
                        lab_name_lst.pop(idx)
                if idx_type == len(idx_sample_type) - 1:
                    records = paragraphs["records"][idx_sample_type[idx_type]:]
                else:
                    records = paragraphs["records"][idx_sample_type[idx_type]: idx_sample_type[idx_type + 1]]
                j = index_unit + 1 
                for k in range(len(lab_number_lst)):
                    test_category = ""
                    for i in range(1, len(records)):
                        try:
                            row_status = check_category(records[i])
                            if row_status == 'Empty row':
                                test_category = ""
                                continue
                            
                            __temp01 = records[i]
                            _tmp = __temp01.split(',')
                            if row_status == 'Category': 
                                __test_category = ",".join(_tmp[0]).replace(',', '') 
                                test_category = __test_category.replace('Metals M7 (NZ MfE)','Metals').replace('Metals M8 (NZ MfE)','Metals')
                                continue
                            
                            item = {
                                "job_lab_number": job_lab_number,
                                "lab_number": lab_number_lst[k],
                                "sample_name": lab_number_lst[k]
                            }
                                       
                            item['test_category'] = test_category
                            item['test_analyte'] = _tmp[0].strip().replace('"', '')
                            item['test_unit'] = _tmp[index_unit].strip()
                            item['test_value'] = _tmp[j].strip().replace('"', '')  
                            item["sample_type"] = sample_type 
                           
                             
                            results.append(item)
                        except:
                            test_category = records[i].strip().replace('"', '')
                    j += 1
        
        
            xx = correct_asbestos_category(asbstos_records=asbstos_records, 
                                           job_lab_number=job_lab_number, 
                                           lab_number_lst=lab_number_lst, 
                                           sample_type=sample_type, 
                                           index_unit = index_unit)
            
            results.extend(xx)
            
            records_df = pd.DataFrame(results)
            records_df = records_df.replace({'"': ''}, regex=True)
            records_df = get_standard_dataframe(records_df)
            
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
                
            def correct_result_value(row):
                try:
                    # If test_value is None return empty
                    if not row['test_value']:
                        return ""
                    
                    if row['test_value'][0].isdigit() or row['test_value'][0]=='<' or row['test_value'][0]=='>':
                        val=str(row['test_value'])
                        val=re.findall("([-+]?\d*\.?\d+)", val)
                        val=val[0]
                    else:
                        val=row['test_value']
                        
                    if row['test_value'][0]=='<' or row['test_value'][0]=='>':
                        val = row['test_value'][0] + val
                        
                    return val
                except:
                    return ""
                
            records_df['resultasnumeric'] = records_df.apply(result_numeric, axis=1)
            records_df['test_value'] = records_df.apply(correct_result_value, axis=1)
            
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
           
            records_df = records_df.sort_values(by='result', ascending=False, na_position='last') 
            records_df = records_df.drop_duplicates(subset=['fileid','labsamplename','labcategoryname','labcategorycode','labtestname','labtestcode','unit', 'labsampletype'], keep='first')
            
            # remove record if labsamplename null
            # records_df.dropna(subset=['labsamplename'], inplace=True)


            records_df = records_df.sort_values(by=['labsamplename','labcategoryname','labtestcode'], ascending=False, na_position='last') 
            
            records_df = records_df.reset_index(drop=True)
            
            records_df_body_data = get_string_buffer_from_dataframe(records_df, False, csv.QUOTE_ALL)
            print(f'records_df_body_data: { records_df_body_data}')
        
            
            records_df.to_csv('my_data.csv', index=False)
            
            return records_df
        
    except Exception as ex:
        print(f'Error method extract_data_records: {ex}')
 
def clean_bad_data(): 
    
    # Đọc dữ liệu từ tệp và thay thế \N bằng khoảng trắng
    with open(r'D:\TnT\research\eurofins\src\csv\datadummy_09102023.csv' , 'r', newline='') as file:
        data = file.read()
        cleaned_data = data.replace('.\n', '. ')

    # Lưu dữ liệu đã tiền xử lý vào tệp mới
    with open(r'D:\TnT\research\eurofins\src\csv\datadummy_09102023_QUAN.csv' , 'w', newline='') as new_file:
        new_file.write(cleaned_data) 

    print("Xử lý xong và đã lưu vào tệp mới.")

def correct_asbstos_logic(records):
    try:
        print(f'Execute correct asbstos test code logic.')
        
        
        asbstos_lab_names= [
                    "Materials Identified", 
                    "Fibres Identified and estimated Asbestos Content (%)",
                    "ACM % asbestos (weighted average)", 
                    "ACM in Soil (as asbestos)",
                    "AF % asbestos (weighted average)", 
                    "Combined AF+FA",
                    "Asbestos Sample Mass/Dimensions", 
                    "Asbestos Reported Result"
                    ]
         
        results = []
        asbstos_results = []
        
        asbstos_results = [row for row in records if any(row.startswith(name) for name in asbstos_lab_names)]
        results =  [row for row in records if not any(row.startswith(name) for name in asbstos_lab_names)] 
        return results, asbstos_results
        

    except Exception as ex:
        print(f'Error method correct_asbstos_logic: {ex}')
        
def get_asbstos_lab_names(asbstos_results: list):
    try:
        result_list = [item for item in asbstos_results if any(item.startswith(start) for start in asbstos_lab_names)]
        return result_list
    except Exception as ex:
        print(f'Error method get_asbstos_lab_names: {ex}')

def correct_asbestos_category(asbstos_records, job_lab_number:str, lab_number_lst: list, sample_type: str, index_unit): 
    """
    Definition:
        - Convert list result category from lab_number_lst
    Args:
        - asbstos_records (list): list data asbstos records
        - job_lab_number (str): name job lad name

    Returns:
        - successResponse: list asbestos category
    """
    
    try:  
        results = []  
        # check Asbestos (Semi-Quantitative) category   
        j = index_unit + 1 
        for k in range(len(lab_number_lst)): 
            # get data for 'Asbestos form' <=> 'Materials Identified' and 'Asbestos type' <=> 'Fibres Identified and estimated Asbestos Content (%)'
            _materials_identifieds = ''
            _tmp_materials_identifieds = []
            filtered_records = list(filter(lambda x: x.startswith('Materials Identified'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _materials_identifieds = filtered_records[0]
                _tmp_materials_identifieds = _materials_identifieds.split(',')
                
            _fibres_identified = ''
            _tmp_fibres_identified = []
            filtered_records = list(filter(lambda x: x.startswith('Fibres Identified and estimated Asbestos Content (%)'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _fibres_identified = filtered_records[0]
                _tmp_fibres_identified = _fibres_identified.split(',')
                
            
                # If materials identified has value
                if _tmp_materials_identifieds:
                    asbstos_types = correct_data_category_asbestos_semi_quantitative(job_lab_number=job_lab_number,
                                                                        lab_number=lab_number_lst[k],
                                                                        sample_name=lab_number_lst[k],
                                                                        sample_type=sample_type, 
                                                                        _materials_identified_value = _tmp_materials_identifieds[j].replace('"', ''), 
                                                                        _tmp_fibres_identified_value = _tmp_fibres_identified[j].replace('"', ''))
                                                                        
                    results.extend(asbstos_types)
            
            
            # get data for 'Asbestos as ACM (w/w%)'
            _acm_asbestos = ''
            _tmp_acm_asbestos = []
            _tmp_acm_asbestos_test_value = ''
            filtered_records = list(filter(lambda x: x.startswith('ACM % asbestos (weighted average)'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _acm_asbestos = filtered_records[0]
                _tmp_acm_asbestos = _acm_asbestos.split(',')
                _tmp_acm_asbestos_test_value = _tmp_acm_asbestos[j].strip().replace('"', '')
                
            _acm_in_soil = ''
            _tmp_acm_in_soil = []
            _acm_in_soil_test_value = ''
            filtered_records = list(filter(lambda x: x.startswith('ACM in Soil (as asbestos)'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _acm_in_soil = filtered_records[0]
                _tmp_acm_in_soil = _acm_in_soil.split(',')
                _acm_in_soil_test_value = _tmp_acm_in_soil[j].strip().replace('"', '')
            
                if _tmp_acm_asbestos_test_value and _tmp_acm_asbestos_test_value.strip().upper() != 'N/A': 
                    item_asbestos_type = {
                    "job_lab_number": job_lab_number,
                    "lab_number": lab_number_lst[k],
                    "sample_name": lab_number_lst[k]
                    }     
                    item_asbestos_type['test_category'] = "New Zealand Guidelines Semi Quantitative Asbestos in Soil" #"Asbestos (Semi-Quantitative)"
                    item_asbestos_type['test_analyte'] = "Asbestos in ACM as % of Total Sample" #"Asbestos as ACM (w/w%)"
                    item_asbestos_type['test_unit'] = ''
                    item_asbestos_type['test_value'] = _acm_in_soil_test_value 
                    item_asbestos_type["sample_type"] = sample_type 
                    results.append(item_asbestos_type)
                else:
                    item_asbestos_type = {
                            "job_lab_number": job_lab_number,
                            "lab_number": lab_number_lst[k],
                            "sample_name": lab_number_lst[k]
                    }     
                    
                    item_asbestos_type['test_category'] = "New Zealand Guidelines Semi Quantitative Asbestos in Soil" #"Asbestos (Semi-Quantitative)"
                    item_asbestos_type['test_analyte'] = "Asbestos in ACM as % of Total Sample" #"Asbestos as ACM (w/w%)"
                    item_asbestos_type['test_unit'] = ''
                    item_asbestos_type['test_value'] = "-" 
                    item_asbestos_type["sample_type"] = sample_type 
                    results.append(item_asbestos_type)
            
                
            # get data for 'Asbestos Fibres/Fine (w/w %)'
            _af_asbestos = ''
            _tmp_af_asbestos = []
            _tmp_af_asbestos_test_value = ''
            filtered_records = list(filter(lambda x: x.startswith('AF % asbestos (weighted average)'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _af_asbestos = filtered_records[0]
                _tmp_af_asbestos = _af_asbestos.split(',')
                _tmp_af_asbestos_test_value = _tmp_af_asbestos[j].strip().replace('"', '')
                
            _combined_af_fa = ''
            _tmp_combined_af_fa = []
            _combined_af_fa_test_value = ''
            filtered_records = list(filter(lambda x: x.startswith('Combined AF+FA'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _combined_af_fa = filtered_records[0]
                _tmp_combined_af_fa = _combined_af_fa.split(',')
                _combined_af_fa_test_value = _tmp_combined_af_fa[j].strip().replace('"', '')
            
                if _tmp_af_asbestos_test_value and _tmp_af_asbestos_test_value.strip().upper() != 'N/A': 
                    item_asbestos_type = {
                    "job_lab_number": job_lab_number,
                    "lab_number": lab_number_lst[k],
                    "sample_name": lab_number_lst[k]
                    }     
                    item_asbestos_type['test_category'] = "New Zealand Guidelines Semi Quantitative Asbestos in Soil" # "Asbestos (Semi-Quantitative)"
                    item_asbestos_type['test_analyte'] = "Combined Fibrous Asbestos + Asbestos Fines as % of Total Sample" #"Asbestos Fibres/Fine (w/w %)"
                    item_asbestos_type['test_unit'] =''
                    item_asbestos_type['test_value'] = _combined_af_fa_test_value 
                    item_asbestos_type["sample_type"] = sample_type 
                    results.append(item_asbestos_type)
                else:
                    item_asbestos_type = {
                            "job_lab_number": job_lab_number,
                            "lab_number": lab_number_lst[k],
                            "sample_name": lab_number_lst[k]
                    }     
                    item_asbestos_type['test_category'] = "New Zealand Guidelines Semi Quantitative Asbestos in Soil" # "Asbestos (Semi-Quantitative)"
                    item_asbestos_type['test_analyte'] = "Combined Fibrous Asbestos + Asbestos Fines as % of Total Sample" #"Asbestos Fibres/Fine (w/w %)"
                    item_asbestos_type['test_unit'] = ''
                    item_asbestos_type['test_value'] = "-" 
                    item_asbestos_type["sample_type"] = sample_type 
                    results.append(item_asbestos_type) 
            
            
            # get asbstos presence_absences category 
            filtered_records = list(filter(lambda x: x.startswith('Asbestos Reported Result'), asbstos_records))
            if filtered_records and len(filtered_records) > 0:
                _asbestos_presence_absences = filtered_records[0]
                _tmp_asbestos_presence_absence = _asbestos_presence_absences.split(',')
                _test_value = _tmp_asbestos_presence_absence[j].strip().replace('"', '')
                _data_category_asbestos_presence_absence = correct_data_category_asbestos_presence_absence(job_lab_number=job_lab_number,
                                                                                lab_number=lab_number_lst[k],
                                                                                sample_name=lab_number_lst[k],
                                                                                sample_type=sample_type, 
                                                                                test_value=_test_value)
                        
                results.extend(_data_category_asbestos_presence_absence)
            j += 1 
                    
        return results 
     
        
    except Exception as ex:
        print(f'Error method correct_asbestos_semi_quantitative: {ex}')
 
        
def correct_data_category_asbestos_semi_quantitative(job_lab_number, lab_number,sample_name, sample_type, _materials_identified_value, _tmp_fibres_identified_value):
    """
    Definition:
        - Get list result asbestos semi quantitative

    Args:
        job_lab_number (str): job lad number
        lab_number (str): job number 
        sample_name (str): sample name
        sample_type (str): sample type
        _materials_identified_value (str): material identifiers identified value
        _tmp_fibres_identified_value (str): tmp fiber identified value

    Returns:
        results: list data category asbestos semi quantitative
    """
    
    try:
        results = [] 
        _asbestos_type = ''
        _asbestos_form = ''
        
        if _tmp_fibres_identified_value and 'asbestos detected' in _tmp_fibres_identified_value:
            if 'Chrysotile' in _tmp_fibres_identified_value and 'Amosite' in _tmp_fibres_identified_value:
                _asbestos_type = 'Chrysotile (White Asbestos) and Amosite (Brown Asbestos) detected)'
            elif 'Chrysotile' in _tmp_fibres_identified_value and 'Amosite' not in _tmp_fibres_identified_value:
                 _asbestos_type = 'Chrysotile (White Asbestos) detected)'
            elif 'Chrysotile' not in _tmp_fibres_identified_value and 'Amosite' in _tmp_fibres_identified_value:
                _asbestos_type = 'Amosite (Brown Asbestos) detected)' 
                 
            _asbestos_form = _materials_identified_value
            
        else:
            _asbestos_type = 'Asbestos NOT detected'
            _asbestos_form = '-'
        
        
        # Create new item_asbestos_type
        item_asbestos_type = {
                    "job_lab_number": job_lab_number,
                    "lab_number": lab_number,
                    "sample_name": sample_name
                }     
        item_asbestos_type['test_category'] = "New Zealand Guidelines Semi Quantitative Asbestos in Soil" # "Asbestos (Semi-Quantitative)"
        item_asbestos_type['test_analyte'] = "Asbestos Presence / Absence" #"Asbestos type"
        item_asbestos_type['test_unit'] = ''
        item_asbestos_type['test_value'] = _asbestos_type 
        item_asbestos_type["sample_type"] = sample_type
        
        results.append(item_asbestos_type)
        
         # Create new _asbestos_form
        item_asbestos_form = {
                    "job_lab_number": job_lab_number,
                    "lab_number": lab_number,
                    "sample_name": sample_name
                }     
        item_asbestos_form['test_category'] = "New Zealand Guidelines Semi Quantitative Asbestos in Soil" #"Asbestos (Semi-Quantitative)"
        item_asbestos_form['test_analyte'] = "Description of Asbestos Form" #"Asbestos form"
        item_asbestos_form['test_unit'] = ''
        item_asbestos_form['test_value'] = _asbestos_form 
        item_asbestos_form["sample_type"] = sample_type
        
        results.append(item_asbestos_form)
        
        return results
    except Exception as ex:
        print(f'Error method correct_data_category_asbestos_presence_absence: {ex}')
        
def correct_data_category_asbestos_presence_absence(job_lab_number, lab_number,sample_name , sample_type, test_value):
    try:
        results = [] 
        _asbestos_type = ''
        _asbestos_form = ''
        
        if test_value and test_value.startswith('No asbestos detected'):
            _asbestos_type = 'Asbestos NOT detected'
            _asbestos_form = '-'
            
        else:
            _asbestos_type = 'Chrysotile (White Asbestos) detected'
            _asbestos_form = correct_asbestos_presence_absence_asbestos_form(test_value=test_value)
        
        
        # Create new item_asbestos_type
        item_asbestos_type = {
                    "job_lab_number": job_lab_number,
                    "lab_number": lab_number,
                    "sample_name": sample_name
                }     
        item_asbestos_type['test_category'] = "Asbestos (Presence/Absence)"
        item_asbestos_type['test_analyte'] = "Asbestos type"
        item_asbestos_type['test_unit'] = ''
        item_asbestos_type['test_value'] = _asbestos_type 
        item_asbestos_type["sample_type"] = sample_type
        
        results.append(item_asbestos_type)
        
         # Create new _asbestos_form
        item_asbestos_form = {
                    "job_lab_number": job_lab_number,
                    "lab_number": lab_number,
                    "sample_name": sample_name
                }     
        item_asbestos_form['test_category'] = "Asbestos (Presence/Absence)"
        item_asbestos_form['test_analyte'] = "Asbestos form"
        item_asbestos_form['test_unit'] = ''
        item_asbestos_form['test_value'] = _asbestos_form 
        item_asbestos_form["sample_type"] = sample_type
        
        results.append(item_asbestos_form)
        
        return results
    except Exception as ex:
        print(f'Error method correct_data_category_asbestos_presence_absence: {ex}')
        
def correct_asbestos_presence_absence_asbestos_form(test_value):
    try:
        if not test_value:
            return "-"
        
        sentences = re.split('\\.', test_value)
        first_sentence = sentences[0]
        
        if not first_sentence:
             return "-"
         
        last_three_words = ' '.join(first_sentence.split()[-3:]).capitalize()
        
        return last_three_words 
        
    except Exception as ex:
        print(f'Error method correct_data_category_asbestos_presence_absence: {ex}')
        return "-"
