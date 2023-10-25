
from common.labs.eurofins import clean_bad_data, extract_eurofins_data
from common.labs.hill import extract_data_hill
import re
import re as regex

def clean_number_in_text(text):  
    # Use regular expression to find all numbers in text
    clean_text = re.sub(r'(\d+\.\d+)', lambda x: x.group(0).replace('.', '|||||||'), text)
    
    print(f'clean_text: {clean_text}')
    
    return clean_text   
  
def correct_asbestos_presence_absence_asbestos_form(test_value):
    """
    Definition:
        - Get last three words to check presence absence asbestos form

    Args:
        test_value (st): col test value

    Returns:
        last_three_words (st): three words to check presence 
    """
    try:
        if not test_value:
            return "-"
        
        # clean number in text
        test_value = clean_number_in_text(test_value)
        
        sentences = re.split('\\.', test_value)
        first_sentence = sentences[0]
        
        if not first_sentence:
             return "-" 
         
        # Get the last three words of the first sentence and capitalize them 
        last_three_words = ' '.join(first_sentence.split()[-3:]).capitalize()
        
        # replace special to decimal
        last_three_words = last_three_words.replace('|||||||', '.')
        
        return last_three_words 
        
    except Exception as ex:
        print(f'Error method correct_data_category_asbestos_presence_absence: {ex}')
        return "-"
    
# def detect_asbestos(text):
#     _asbestos_type = 'Asbestos NOT detected'
#     _asbestos_form = '-'
    
#     if not text or 'N/A' in text:
#         return _asbestos_type, _asbestos_form
        
#     asbestos_types = ['CHRYSOTILE', 'AMOSITE', 'CROCIDOLITE']
#     detected = []
#     uppercase_text = text.upper() 

#     for asbestos in asbestos_types:
#         if asbestos in uppercase_text:
#             detected.append(asbestos)

#     if len(detected) == 0:
#         _asbestos_type = 'Asbestos Not detected'
#     elif len(detected) == 1:
#         if 'CHRYSOTILE' in detected:
#             _asbestos_type =  'Chrysotile (White Asbestos) detected'
#         elif 'AMOSITE' in detected:
#             _asbestos_type =  'Amosite (Brown Asbestos) detected'
#         elif 'CROCIDOLITE' in detected:
#             _asbestos_type =  'Crocidolite Asbestos detected'
#     elif len(detected) == 2:
#         if 'CHRYSOTILE' in detected and 'AMOSITE' in detected:
#             _asbestos_type =  'Chrysotile (White Asbestos) and Amosite (Brown Asbestos) detected'
#         elif 'AMOSITE' in detected and 'CROCIDOLITE' in detected:
#             _asbestos_type =  'Amosite (Brown Asbestos) and Crocidolite Asbestos detected'
#         elif 'CHRYSOTILE' in detected and 'CROCIDOLITE' in detected:
#             _asbestos_type =  'Chrysotile (White Asbestos) and Crocidolite Asbestos detected'
#     elif len(detected) == 3:
#         _asbestos_type =  'Chrysotile (White Asbestos) and Amosite (Brown Asbestos) and Crocidolite Asbestos detected'
        
    
#     # get _asbestos_form value
#     if _asbestos_type != "Asbestos NOT detected":
#         _asbestos_form = correct_asbestos_presence_absence_asbestos_form(text)
        
#     # return data _asbestos_type, _asbestos_form
#     return _asbestos_type, _asbestos_form


def detect_asbestos(text):
    """Detects the presence of asbestos in a given text.

    Args:
        text: A string containing the text to be analyzed.

    Returns:
        A tuple of two strings, (_asbestos_type, _asbestos_form), where:
            _asbestos_type: A string indicating the type of asbestos detected, or
                "Asbestos NOT detected" if no asbestos is detected.
            _asbestos_form: A string indicating the form of asbestos detected, or
                "-" if no asbestos is detected.
    """

    # Check if the text is empty or contains "N/A".
    if not text or "N/A" in text:
        return "Asbestos NOT detected", "-"

    # Convert the text to uppercase.
    uppercase_text = text.upper()

    # Create a list of the asbestos types that are detected.
    detected = [asbestos_type for asbestos_type in ["CHRYSOTILE", "AMOSITE", "CROCIDOLITE"] if asbestos_type in uppercase_text]

    # Determine the asbestos type and form based on the number of asbestos types
    # that were detected.
    asbestos_type = "Asbestos NOT detected"
    asbestos_form = "-"
    if detected:
        asbestos_type = ", ".join(detected)
        asbestos_form = correct_asbestos_presence_absence_asbestos_form(text)

    return asbestos_type, asbestos_form


def extract_sample_name(sample_name):
    """
    Definition:
        - Get date as string type from Sample name by regex

    Inputs:
        sample_name (str)

    Returns:
        str: Sample name
        str: Depth
        str: Date as string type if not match date regex return 'none'
    """
    print(f'sample name inputs: {sample_name}')
    sample_name_extracted = ""
    date_extracted = ""
    depth_extracted = "none"

    DATE_REGEX = r"\d{2,}.[A-Za-z]{3,9}.+\d{4}"
    DEPTH_REGEX = r"(\d+\.\d+m)|(\d+m)|(\d+\-\d+m)|(\d+\_\d+m)"

    date_match = regex.findall(DATE_REGEX, sample_name)
    if len(date_match) > 0:
        date_extracted = date_match[0]
        date_start_index = sample_name.find(date_extracted)
        # Update Sample name input
        sample_name = sample_name[0:date_start_index]
        # print(f'pass date_extracted: {date_extracted}')
        # print(f'sample name: {sample_name}')

    try:
        depth_match = regex.findall(DEPTH_REGEX, sample_name)
        if len(depth_match) > 0:
            len_of_current_sample = len(sample_name)
            start_depth_extracted = list(depth_match[0])
            start_depth_text = ""
            for item in start_depth_extracted:
                if "m" in item:
                    start_depth_text = item
                    start_depth_index = sample_name.find(start_depth_text)
                    depth_extracted = sample_name[start_depth_index:len_of_current_sample]
                    # print(f'pass depth_extracted: {depth_extracted}')
                    depth_text_index = sample_name.find(depth_extracted)
                    # Update Sample name input
                    sample_name = sample_name[0:depth_text_index]
                    # print(f'sample name: {sample_name}')
                    break
    except Exception as e:
        print(f'handler: {e}')
        raise e
    finally:
        sample_name_extracted = sample_name
        # split last character if it is specical case
        if sample_name[-1] == '_' or sample_name[-1] == '-':
            sample_name_extracted = sample_name[:-1]
        print(f'depth_extracted: {depth_extracted}')
        print(f'date_extracted: {date_extracted}')

    return sample_name_extracted, depth_extracted, date_extracted

if __name__ == "__main__": 
    
    extract_eurofins_data()
    #sample_name_extracted, depth_extracted, date_extracted = extract_sample_name('13ASH_CHA01_0.0')
    # text_array = [
    #     "1. Crocidolite, Crocidolite, Crocidolite, Amosite, No asbestos fibers were found in this sample.",
    #     "2. This Chrysotile sample contains Chrysotile fibers.",
    #     "3. Crocidolite Amosite Amosite Chrysotile fibers were detected in this sample.",
    #     "4. Crocidolite is present in the sample.",
    #     "5. Chrysotile and Amosite fibers were detected in this sample.",
    #     "6. Amosite and Crocidolite were found in the sample.",
    #     "7. Chrysotile and Crocidolite fibers were detected.",
    #     "8. This sample contains Chrysotile, Amosite, and Crocidolite fibers.",
    #     "9. Chrysotile fibers were found alongside Chrysotile dust."
    # ]

    # for i, text in enumerate(text_array):
    #     _asbestos_type = detect_asbestos(text)
    #     print(f"Test case {i + 1}: {_asbestos_type}")
 