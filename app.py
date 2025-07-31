from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, session
import json
import os
import logging
from datetime import datetime
import uuid
import pandas as pd
import ast
from functools import wraps
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('audit_tool.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set the base directory for all data files
DATA_DIR = "/Users/kiranbeethoju/IPAuditTool_ICD"

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Admin required decorator
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        
        # Check if user is admin
        user_name = session.get('user_name')
        user_team = session.get('user_team', '')
        
        # Allow access if user is admin or in Administration team
        if user_name == 'admin' or user_team == 'Administration':
            return f(*args, **kwargs)
        else:
            flash('Access denied. Admin privileges required.', 'error')
            return redirect(url_for('index'))
    return decorated_function

# Load auditor credentials
def load_auditor_credentials():
    """Load auditor credentials from logins.xlsx"""
    try:
        login_file = os.path.join(DATA_DIR, 'logins.xlsx')
        if os.path.exists(login_file):
            df = pd.read_excel(login_file)
            credentials = {}
            for _, row in df.iterrows():
                credentials[row['username']] = {
                    'password': row['password'],
                    'name': row['name'],
                    'team': row['team']
                }
            logger.info(f"Loaded {len(credentials)} auditor credentials")
            return credentials
        else:
            logger.warning("logins.xlsx not found, creating default admin account")
            return {
                'admin': {
                    'password': 'admin123',
                    'name': 'Admin User',
                    'team': 'Administration'
                }
            }
    except Exception as e:
        logger.error(f"Error loading auditor credentials: {e}")
        return {}

# Load credentials on startup
AUDITOR_CREDENTIALS = load_auditor_credentials()

# Custom Jinja2 filter for flattening lists
@app.template_filter("flatten")
def flatten_filter(lst):
    """Flatten a list of lists"""
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten_filter(item))
        else:
            result.append(item)
    return result

@app.template_filter("strip")
def strip_filter(value):
    """Strip whitespace from a string"""
    if value is None:
        return ""
    return str(value).strip()

@app.template_filter("check_sdx_match")
def check_sdx_match_filter(icd_code, account_num, csv_data):
    """Template filter to check SDX match and return ground truth codes"""
    try:
        # Convert to string if needed
        if icd_code is None:
            return {"status": "SDX Mismatch", "gt_codes": []}
        
        icd_code = str(icd_code)
        account_num = str(account_num)
        
        # Debug logging
        logger.info(f"check_sdx_match_filter called with: icd_code={icd_code}, account_num={account_num}, csv_data_type={type(csv_data)}")
        
        # Handle csv_data structure: {"pdx": [...], "sdx": [...]}
        if isinstance(csv_data, dict):
            # Get ground truth SDX codes for this account
            ground_truth_sdx = csv_data.get("sdx", [])
        else:
            logger.warning(f"Unexpected csv_data type: {type(csv_data)}")
            return {"status": "SDX Mismatch", "gt_codes": []}
        
        # Extract ICD codes from ground truth data
        ground_truth_codes = []
        if isinstance(ground_truth_sdx, list):
            for item in ground_truth_sdx:
                if isinstance(item, dict):
                    icd = item.get('icd_code', '')
                    if icd:
                        ground_truth_codes.append(str(icd))
        
        logger.info(f"Found {len(ground_truth_codes)} ground truth codes for account {account_num}: {ground_truth_codes}")
        
        if not icd_code or not ground_truth_codes:
            return {"status": "SDX Mismatch", "gt_codes": ground_truth_codes}
        
        # Direct match
        if icd_code in ground_truth_codes:
            return {"status": "SDX Match", "gt_codes": [icd_code]}
        
        # Check for specificity issues (partial matches)
        specificity_matches = []
        for gt_code in ground_truth_codes:
            if len(icd_code) > 1 and len(gt_code) > 1:
                # Check if removing last character makes them match
                if icd_code[:-1] == gt_code[:-1]:
                    specificity_matches.append(gt_code)
                # Check if one is a prefix of the other
                elif icd_code.startswith(gt_code) or gt_code.startswith(icd_code):
                    specificity_matches.append(gt_code)
        
        if specificity_matches:
            return {"status": "Specificity Issue", "gt_codes": specificity_matches}
        
        return {"status": "SDX Mismatch", "gt_codes": ground_truth_codes}
    except Exception as e:
        logger.error(f"Error in check_sdx_match_filter: {e}")
        return {"status": "SDX Mismatch", "gt_codes": []}

@app.template_filter("get_submitted_feedback")
def get_submitted_feedback_filter(account_id):
    """Template filter to get submitted feedback for an account with type information"""
    try:
        feedback_file = os.path.join(DATA_DIR, 'feedback_data.xlsx')
        if not os.path.exists(feedback_file):
            logger.warning(f"Feedback file not found: {feedback_file}")
            return []
        
        df = pd.read_excel(feedback_file)
        logger.info(f"Loaded feedback data with {len(df)} rows")
        
        account_feedback = df[df['account_number'].astype(str) == str(account_id)]
        logger.info(f"Found {len(account_feedback)} feedback entries for account {account_id}")
        
        # Load parsed data to determine correct types for each ICD code
        parsed_data = load_parsed_results_data()
        account_parsed_data = parsed_data.get(account_id, {})
        combination_code_review = account_parsed_data.get('combination_code_review', [])
        
        # Create mapping of ICD codes to their types
        icd_type_mapping = {}
        for item in combination_code_review:
            icd_code = item.get('icd-10-cm code', '')
            diagnosis_type = item.get('type', '').upper()
            if icd_code:
                icd_type_mapping[icd_code] = diagnosis_type
        
        feedback_list = []
        for _, row in account_feedback.iterrows():
            icd_code = row['icd_code']
            
            # Determine the type based on the parsed data mapping
            feedback_type = "ICD Feedback"  # Default type
            if icd_code in icd_type_mapping:
                diagnosis_type = icd_type_mapping[icd_code]
                if diagnosis_type == 'PDX':
                    feedback_type = "PDX Feedback"
                elif diagnosis_type == 'ADX':
                    feedback_type = "ADX Feedback"
                elif diagnosis_type == 'SDX':
                    feedback_type = "SDX Feedback"
            
            feedback_item = {
                'icd_code': row['icd_code'],
                'type': feedback_type,
                'status': row['accept_reject'],
                'feedback': row['feedback_comment'],
                'reviewer': row['reviewer'],
                'timestamp': row['timestamp'],
                'cc_mcc': row.get('cc_mcc', ''),  # Include CC/MCC information
                'pdx_feedback': row.get('pdx_feedback', ''),  # Include general feedback
                'adx_feedback': row.get('adx_feedback', ''),
                'sdx_feedback': row.get('sdx_feedback', '')
            }
            feedback_list.append(feedback_item)
            logger.info(f"Added feedback item: {feedback_item}")
        
        logger.info(f"Returning {len(feedback_list)} feedback items for account {account_id}")
        return feedback_list
    except Exception as e:
        logger.error(f"Error getting submitted feedback: {e}")
        return []

def safe_parse_array(data_string):
    """Safely parse array of dictionaries using multiple methods"""
    try:
        if not data_string or data_string == '-':
            return []
        
        # Handle float/int values - convert to string first
        if isinstance(data_string, (float, int)):
            data_string = str(data_string)
        
        # Clean up the string
        if isinstance(data_string, str):
            data_string = data_string.strip()
        else:
            logger.warning(f"Unexpected data type for parsing: {type(data_string)}")
            return []
        
        # Try ast.literal_eval first (safest)
        try:
            parsed_data = ast.literal_eval(data_string)
            if isinstance(parsed_data, list):
                logger.info(f"Successfully parsed array using ast.literal_eval")
                return parsed_data
        except (ValueError, SyntaxError):
            pass
        
        # Try json.loads if ast.literal_eval fails
        try:
            parsed_data = json.loads(data_string)
            if isinstance(parsed_data, list):
                logger.info(f"Successfully parsed array using json.loads")
                return parsed_data
        except (ValueError, json.JSONDecodeError):
            pass
        
        # Fallback to manual parsing if both fail
        logger.warning(f"Both ast.literal_eval and json.loads failed, using manual parsing")
        return parse_array_of_objects(data_string)
        
    except Exception as e:
        logger.error(f"Error parsing array: {e}")
        return []

def parse_array_of_objects(data_string):
    """Parse array of objects from string representation (fallback method)"""
    try:
        if not data_string or data_string == '-':
            return []
        
        # Handle float/int values - convert to string first
        if isinstance(data_string, (float, int)):
            data_string = str(data_string)
        
        # Clean up the string and parse it
        if isinstance(data_string, str):
            data_string = data_string.strip()
        else:
            logger.warning(f"Unexpected data type for parsing: {type(data_string)}")
            return []
            
        if data_string.startswith('[') and data_string.endswith(']'):
            # Remove outer brackets and split by '], ['
            content = data_string[1:-1]
            objects = []
            
            # Simple parsing for the array structure
            # Split by '], [' to separate objects
            if '], [' in content:
                object_strings = content.split('], [')
            else:
                object_strings = [content]
            
            for obj_str in object_strings:
                obj = {}
                # Parse key-value pairs
                pairs = obj_str.split('", "')
                for pair in pairs:
                    if '": "' in pair:
                        key, value = pair.split('": "', 1)
                        key = key.strip('"')
                        value = value.strip('"')
                        obj[key] = value
                    elif '": ' in pair and not pair.startswith('"'):
                        # Handle cases without quotes
                        if '": ' in pair:
                            key, value = pair.split('": ', 1)
                            key = key.strip('"')
                            value = value.strip('"')
                            obj[key] = value
                
                if obj:
                    objects.append(obj)
            
            return objects
        else:
            return []
    except Exception as e:
        logger.error(f"Error parsing array of objects: {e}")
        return []

def load_icd_review_data_from_parsed():
    """Load ICD review data from parsed A. Coded Diagnoses Table"""
    try:
        if os.path.exists(os.path.join(DATA_DIR, 'parsed_results_new_v6.xlsx')):
            df = pd.read_excel(os.path.join(DATA_DIR, 'parsed_results_new_v6.xlsx'))
            icd_data = {}
            
            for _, row in df.iterrows():
                account_num = str(row['acct_number'])
                coded_diagnoses_str = row.get('A. Coded Diagnoses Table', '')
                
                # Parse the array of dictionaries
                coded_diagnoses = safe_parse_array(coded_diagnoses_str)
                
                if account_num not in icd_data:
                    icd_data[account_num] = []
                
                # Convert parsed diagnoses to review format
                for diagnosis in coded_diagnoses:
                    review_item = {
                        'icd_code': diagnosis.get('ICD-10-CM Code', ''),
                        'icd_description': diagnosis.get('Diagnosis/Condition', ''),
                        'type': diagnosis.get('Type', ''),  # PDX, ADX, SDX
                        'cc_mcc': diagnosis.get('CC/MCC', ''),
                        'poa': diagnosis.get('POA', ''),
                        'supporting_documentation': diagnosis.get('Supporting Documentation & Location', ''),
                        'coding_clinic_reference': diagnosis.get('Coding Clinic/Guideline Reference', '')
                    }
                    icd_data[account_num].append(review_item)
            
            logger.info(f"Loaded ICD review data for {len(icd_data)} accounts from parsed diagnoses")
            return icd_data
        else:
            logger.warning(f"parsed_results_new_v6.xlsx not found in {DATA_DIR}")
            return {}
    except Exception as e:
        logger.error(f"Error loading ICD review data from parsed: {e}")
        return {}

def load_audited_accounts_data():
    """Load data from audited_accounts_129.xlsx file"""
    try:
        if os.path.exists(os.path.join(DATA_DIR, 'audited_accounts_129.xlsx')):
            df = pd.read_excel(os.path.join(DATA_DIR, 'audited_accounts_129.xlsx'))
            accounts_data = {}
            
            for _, row in df.iterrows():
                account_num = str(row['ACCOUNT_NUMBER'])
                document_content = str(row.get('document', ''))
                
                accounts_data[account_num] = {
                    'account_number': account_num,
                    'facility_name': row.get('FAC_NAME', ''),
                    'admission_datetime': row.get('ADM_DATETIME', ''),
                    'discharge_datetime': row.get('DISCH_DATETIME', ''),
                    'document': document_content,
                    'update_date': row.get('UPDATE_DATE', ''),
                    'trans_date': row.get('TRANS_DATE', ''),
                    'edit_date': row.get('EDIT_DATE', ''),
                    'day_diff': row.get('DAY_DIFF', '')
                }
            
            logger.info(f"Loaded audited accounts data for {len(accounts_data)} accounts")
            return accounts_data
        else:
            logger.warning(f"audited_accounts_129.xlsx not found in {DATA_DIR}")
            return {}
    except Exception as e:
        logger.error(f"Error loading audited accounts data: {e}")
        return {}

def load_documents_from_excel():
    """Load documents from audited_accounts_129.xlsx file"""
    try:
        if os.path.exists(os.path.join(DATA_DIR, 'audited_accounts_129.xlsx')):
            df = pd.read_excel(os.path.join(DATA_DIR, 'audited_accounts_129.xlsx'))
            documents = {}
            
            logger.info(f"Loading documents from audited_accounts_129.xlsx with columns: {list(df.columns)}")
            
            for _, row in df.iterrows():
                account_num = str(row['ACCOUNT_NUMBER'])
                
                # Try to get content from combined_content column first, then fallback to document
                document_content = str(row.get('combined_content', row.get('document', '')))
                
                if account_num not in documents:
                    documents[account_num] = {}
                
                # Create a single document entry from the combined_content column
                documents[account_num]['Combined Document'] = {
                    'title': 'Combined Document',
                    'content': document_content,
                    'color': '#2a2a2a'
                }
                
                # Log the first few accounts for debugging
                if len(documents) <= 3:
                    logger.info(f"Account {account_num} document content preview: {document_content[:100]}...")
            
            logger.info(f"Loaded documents for {len(documents)} accounts from audited_accounts_129.xlsx")
            return documents
        else:
            logger.warning(f"audited_accounts_129.xlsx not found in {DATA_DIR}")
            return {}
    except Exception as e:
        logger.error(f"Error loading documents from Excel: {e}")
        return {}

def load_coded_vs_llm_comparison():
    """Load coded vs LLM comparison data from PDX and SDX CSV files and map against parsed data"""
    try:
        comparison_data = {}
        
        # Load PDX data
        if os.path.exists(os.path.join(DATA_DIR, 'pdx_gt.csv')):
            pdx_df = pd.read_csv(os.path.join(DATA_DIR, 'pdx_gt.csv'))
            logger.info(f"Loading comparison PDX data with columns: {list(pdx_df.columns)}")
            for _, row in pdx_df.iterrows():
                # Handle different column names
                acct_col = 'acct_number' if 'acct_number' in pdx_df.columns else 'account_number'
                icd_col = 'icd_code' if 'icd_code' in pdx_df.columns else 'ICD_CODE'
                
                account_num = str(row[acct_col])
                if account_num not in comparison_data:
                    comparison_data[account_num] = []
                
                comparison_data[account_num].append({
                    'type': 'PDX',
                    'coded_icd': row[icd_col],
                    'coded_description': f"PDX - {row[icd_col]}",  # Will be updated with parsed data
                    'llm_icd': row[icd_col],  # For now, using same as coded
                    'llm_description': f"PDX - {row[icd_col]}",  # Will be updated with parsed data
                    'match_status': 'Match'  # Default to match for now
                })
        
        # Load SDX data
        if os.path.exists(os.path.join(DATA_DIR, 'sdx_gt.csv')):
            sdx_df = pd.read_csv(os.path.join(DATA_DIR, 'sdx_gt.csv'))
            logger.info(f"Loading comparison SDX data with columns: {list(sdx_df.columns)}")
            for _, row in sdx_df.iterrows():
                # Handle different column names
                acct_col = 'acct_number' if 'acct_number' in sdx_df.columns else 'account_number'
                icd_col = 'icd_code' if 'icd_code' in sdx_df.columns else 'ICD_CODE'
                
                account_num = str(row[acct_col])
                if account_num not in comparison_data:
                    comparison_data[account_num] = []
                
                comparison_data[account_num].append({
                    'type': 'SDX',
                    'coded_icd': row[icd_col],
                    'coded_description': f"SDX - {row[icd_col]}",  # Will be updated with parsed data
                    'llm_icd': row[icd_col],  # For now, using same as coded
                    'llm_description': f"SDX - {row[icd_col]}",  # Will be updated with parsed data
                    'match_status': 'Match'  # Default to match for now
                })
        
        logger.info(f"Loaded coded vs LLM comparison data for {len(comparison_data)} accounts from CSV files.")
        return comparison_data
    except Exception as e:
        logger.error(f"Error loading coded vs LLM comparison data: {e}")
        return {}

def load_parsed_results_data():
    """Load and parse the parsed_results_new_v6.xlsx file"""
    try:
        if os.path.exists(os.path.join(DATA_DIR, 'parsed_results_new_v6.xlsx')):
            df = pd.read_excel(os.path.join(DATA_DIR, 'parsed_results_new_v6.xlsx'))
            parsed_data = {}
            
            for _, row in df.iterrows():
                account_num = str(row['acct_number'])
                
                # Parse the array of objects for each column
                coded_diagnoses = safe_parse_array(row.get('A. Coded Diagnoses Table', ''))
                coded_procedures = safe_parse_array(row.get('B. Coded Procedures Table', ''))
                ms_drg_assignment = safe_parse_array(row.get('C. MS-DRG Assignment and Rationale', ''))
                combination_code_review = safe_parse_array(row.get('comb', ''))
                # Debug logging for CC/MCC values
                if combination_code_review:
                    logger.info(f"Combination code review data for account {account_num}:")
                    for item in combination_code_review:
                        logger.info(f"  Type: {item.get('type')}, ICD: {item.get('icd-10-cm code')}, CC/MCC: {item.get('cc/mcc')}")
                
                parsed_data[account_num] = {
                    'coded_diagnoses': coded_diagnoses,
                    'coded_procedures': coded_procedures,
                    'ms_drg_assignment': ms_drg_assignment,
                    'combination_code_review': combination_code_review
                }
            
            logger.info(f"Loaded parsed results data for {len(parsed_data)} accounts")
            return parsed_data
        else:
            logger.warning(f"parsed_results_new_v6.xlsx not found in {DATA_DIR}")
            return {}
    except Exception as e:
        logger.error(f"Error loading parsed results data: {e}")
        return {}

def merge_comparison_with_parsed_data(comparison_data, parsed_data):
    """Merge comparison data with parsed diagnoses and map against ground truth SDX codes"""
    try:
        merged_data = {}
        
        for account_num, comparison_items in comparison_data.items():
            merged_data[account_num] = []
            parsed_diagnoses = parsed_data.get(account_num, {}).get('coded_diagnoses', [])
            
            # Get ground truth SDX codes for this account
            ground_truth_sdx_codes = set()
            for item in comparison_items:
                if item.get('type') == 'SDX':
                    ground_truth_sdx_codes.add(item.get('coded_icd', ''))
            
            # Create comprehensive mapping for all parsed diagnoses
            all_parsed_codes = []
            for diagnosis in parsed_diagnoses:
                icd_code = diagnosis.get('ICD-10-CM Code', '')
                diagnosis_type = diagnosis.get('Type', '').upper()
                description = diagnosis.get('Diagnosis/Condition', '')
                
                if icd_code:
                    # Check if this code exists in ground truth
                    is_in_ground_truth = icd_code in ground_truth_sdx_codes
                    match_status = 'Match' if is_in_ground_truth else 'Mismatch'
                    
                    all_parsed_codes.append({
                        'type': diagnosis_type,
                        'coded_icd': icd_code,
                        'coded_description': description,
                        'llm_icd': icd_code,  # LLM generated this code
                        'llm_description': description,  # LLM generated this description
                        'match_status': match_status  # Match if in ground truth, Mismatch if not
                    })
            
            # Add all parsed codes to comparison data
            merged_data[account_num] = all_parsed_codes
        
        logger.info(f"Merged comparison data with parsed diagnoses for {len(merged_data)} accounts")
        return merged_data
    except Exception as e:
        logger.error(f"Error merging comparison data: {e}")
        return comparison_data

def load_csv_data():
    """Load data from CSV files (pdx_gt_new.csv, sdx_gt_new.csv)"""
    try:
        csv_data = {}
        
        # Load PDX data - try new files first, then fallback to old
        pdx_file_paths = [
            os.path.join(DATA_DIR, 'pdx_gt_new.csv'),
            os.path.join(DATA_DIR, 'pdx_gt.csv')
        ]
        
        pdx_data = {}
        pdx_loaded = False
        
        for pdx_file in pdx_file_paths:
            if os.path.exists(pdx_file):
                pdx_df = pd.read_csv(pdx_file)
                logger.info(f"Loading PDX data from {pdx_file}")
                logger.info(f"PDX columns: {list(pdx_df.columns)}")
                
                # Handle different column names
                acct_col = 'acct_number' if 'acct_number' in pdx_df.columns else 'account_number'
                icd_col = 'icd_code' if 'icd_code' in pdx_df.columns else 'ICD_CODE'
                
                for _, row in pdx_df.iterrows():
                    acct_num = str(row[acct_col])
                    if acct_num not in pdx_data:
                        pdx_data[acct_num] = []
                    pdx_data[acct_num].append({
                        'icd_code': row[icd_col],
                        'pat_enc_id': row.get('pat_enc_id', ''),
                        'icd_term': row.get('icd_term', ''),
                        'ccmcc': row.get('ccmcc', ''),
                        'SOI': row.get('SOI', ''),
                        'ROM': row.get('ROM', '')
                    })
                csv_data['pdx'] = pdx_data
                logger.info(f"Loaded PDX data for {len(pdx_data)} accounts from {pdx_file}")
                pdx_loaded = True
                break
        
        if not pdx_loaded:
            logger.warning("No PDX file found")
        
        # Load SDX data - try new files first, then fallback to old
        sdx_file_paths = [
            os.path.join(DATA_DIR, 'sdx_gt_new.csv'),
            os.path.join(DATA_DIR, 'sdx_gt.csv')
        ]
        
        sdx_data = {}
        sdx_loaded = False
        
        for sdx_file in sdx_file_paths:
            if os.path.exists(sdx_file):
                sdx_df = pd.read_csv(sdx_file)
                logger.info(f"Loading SDX data from {sdx_file}")
                logger.info(f"SDX columns: {list(sdx_df.columns)}")
                
                # Handle different column names
                acct_col = 'acct_number' if 'acct_number' in sdx_df.columns else 'account_number'
                icd_col = 'icd_code' if 'icd_code' in sdx_df.columns else 'ICD_CODE'
                
                for _, row in sdx_df.iterrows():
                    acct_num = str(row[acct_col])
                    if acct_num not in sdx_data:
                        sdx_data[acct_num] = []
                    sdx_data[acct_num].append({
                        'icd_code': row[icd_col],
                        'pat_enc_id': row.get('pat_enc_id', ''),
                        'icd_term': row.get('icd_term', ''),
                        'cc_mcc': row.get('ccmcc', ''),  # Use ccmcc column for CC/MCC
                        'SOI': row.get('SOI', ''),
                        'ROM': row.get('ROM', '')
                    })
                csv_data['sdx'] = sdx_data
                logger.info(f"Loaded SDX data for {len(sdx_data)} accounts from {sdx_file}")
                sdx_loaded = True
                break
        
        if not sdx_loaded:
            logger.warning("No SDX file found")
        
        return csv_data
    except Exception as e:
        logger.error(f"Error loading CSV data: {e}")
        return {}

def save_feedback_to_excel(account_id, feedback_data):
    """Save feedback data to Excel file - store each entry individually without deduplication"""
    try:
        feedback_file = os.path.join(DATA_DIR, 'feedback_data.xlsx')
        
        # Load existing data
        if os.path.exists(feedback_file):
            df = pd.read_excel(feedback_file)
        else:
            df = pd.DataFrame(columns=['account_number', 'icd_code', 'accept_reject', 
                                       'feedback_comment', 'pdx_feedback', 'adx_feedback', 
                                       'sdx_feedback', 'reviewer', 'timestamp', 'entry_id', 'cc_mcc'])

        # Get data from feedback_data
        pdx_feedback = feedback_data.get('pdx_feedback', '')
        adx_feedback = feedback_data.get('adx_feedback', '')
        sdx_feedback = feedback_data.get('sdx_feedback', '')
        reviewer = feedback_data.get('reviewer', 'default_auditor')
        
        # Generate unique entry IDs for this submission
        timestamp = datetime.now().isoformat()
        
        # Add new ICD feedback entries - each gets its own row regardless of ICD code
        for index, item in enumerate(feedback_data.get('icd_feedback', [])):
            icd_code = item['icd_code']
            cc_mcc = item.get('cc_mcc', '')  # Get CC/MCC information from feedback item
            entry_id = f"{account_id}_{timestamp}_{index}"  # Unique identifier for each entry
            
            new_icd_feedback = {
                'account_number': account_id,
                'icd_code': icd_code,
                'accept_reject': item['status'],
                'feedback_comment': item['feedback'],
                'pdx_feedback': pdx_feedback,
                'adx_feedback': adx_feedback,
                'sdx_feedback': sdx_feedback,
                'reviewer': reviewer,
                'timestamp': timestamp,
                'entry_id': entry_id,
                'cc_mcc': cc_mcc
            }
            df = pd.concat([df, pd.DataFrame([new_icd_feedback])], ignore_index=True)
            
        logger.info(f"Saved feedback for account {account_id} with {len(feedback_data.get('icd_feedback', []))} entries")
        
        # Save back to Excel
        df.to_excel(feedback_file, index=False)
        
    except Exception as e:
        logger.error(f"Error saving feedback to Excel: {e}")

def calculate_account_statistics(account_num, parsed_data, csv_data, comparison_data):
    """Calculate account statistics dynamically from actual data"""
    try:
        # Get parsed diagnoses for this account
        coded_diagnoses = parsed_data.get(account_num, {}).get('coded_diagnoses', [])
        
        # Get ground truth SDX data
        ground_truth_sdx = csv_data.get("sdx", {}).get(account_num, [])
        ground_truth_sdx_codes = {item['icd_code'] for item in ground_truth_sdx}
        
        # Calculate SDX statistics - FIXED: Total SDX should be from ground truth, not LLM
        total_sdx = len(ground_truth_sdx)  # Ground truth SDX count
        
        # Get LLM extracted SDX data
        sdx_diagnoses = [d for d in coded_diagnoses if d.get('Type', '').upper() == 'SDX']
        parsed_sdx_codes = {d.get('ICD-10-CM Code', '') for d in sdx_diagnoses}
        
        # Calculate matched SDX (SDX codes that exist in both parsed and ground truth)
        matched_sdx = len(parsed_sdx_codes.intersection(ground_truth_sdx_codes))
        
        # Calculate total matched ICDs from comparison data
        matched_icds = len(comparison_data.get(account_num, []))
        
        # Calculate accuracy based on matched vs total
        total_icds = len(coded_diagnoses)
        accuracy = round((matched_icds / total_icds * 100) if total_icds > 0 else 0, 1)
        
        # Get PDX information - FIXED: LLM PDX and Coded PDX were swapped
        # Get LLM PDX from parsed data (this is the LLM-generated PDX)
        pdx_diagnoses = [d for d in coded_diagnoses if d.get('Type', '').upper() == 'PDX']
        pdx_llm = pdx_diagnoses[0].get('ICD-10-CM Code', 'N/A') if pdx_diagnoses else 'N/A'
        
        # Get Coded PDX from ground truth (this is the actual coded PDX)
        ground_truth_pdx = csv_data.get("pdx", {}).get(account_num, [])
        pdx_coded = ground_truth_pdx[0].get('icd_code', 'N/A') if ground_truth_pdx else 'N/A'
        
        # Check if PDX matches
        pdx_matched = pdx_coded == pdx_llm and pdx_coded != 'N/A'
        
        # Calculate CC/MCC statistics
        cc_mcc_stats = calculate_cc_mcc_statistics(account_num, parsed_data, csv_data)
        
        return {
            'total_sdx': int(total_sdx),
            'llm_matched_sdx': int(matched_sdx),
            'matched_icds': int(matched_icds),
            'accuracy': float(accuracy),
            'pdx_coded': str(pdx_coded),
            'pdx_llm': str(pdx_llm),
            'pdx_matched': bool(pdx_matched),
            'cc_mcc_stats': cc_mcc_stats
        }
    except Exception as e:
        logger.error(f"Error calculating statistics for account {account_num}: {e}")
        return {
            'total_sdx': int(0),
            'llm_matched_sdx': int(0),
            'matched_icds': int(0),
            'accuracy': float(0),
            'pdx_coded': str('N/A'),
            'pdx_llm': str('N/A'),
            'pdx_matched': bool(False),
            'cc_mcc_stats': {'gt_cc': 0, 'gt_mcc': 0, 'llm_cc': 0, 'llm_mcc': 0}
        }

def calculate_cc_mcc_statistics(account_num, parsed_data, csv_data):
    """Calculate CC/MCC statistics comparing ground truth vs LLM extraction"""
    try:
        # Get ground truth SDX data with CC/MCC information
        ground_truth_sdx = csv_data.get("sdx", {}).get(account_num, [])
        
        # Debug logging for ground truth data
        logger.info(f"Ground truth SDX data for account {account_num}: {ground_truth_sdx}")
        
        # Count CC/MCC in ground truth SDX
        gt_cc_count = 0
        gt_mcc_count = 0
        
        for item in ground_truth_sdx:
            # Try different possible column names for CC/MCC
            cc_mcc = None
            if 'cc_mcc' in item:
                cc_mcc = str(item['cc_mcc']).upper() if item['cc_mcc'] else ''
            elif 'ccmcc' in item:
                cc_mcc = str(item['ccmcc']).upper() if item['ccmcc'] else ''
            
            if cc_mcc == 'CC':
                gt_cc_count += 1
                logger.info(f"Found CC in ground truth: {item}")
            elif cc_mcc == 'MCC':
                gt_mcc_count += 1
                logger.info(f"Found MCC in ground truth: {item}")
        
        # Get LLM extracted SDX data with CC/MCC information
        llm_sdx_diagnoses = []
        if account_num in parsed_data:
            combination_code_review = parsed_data[account_num].get('combination_code_review', [])
            for item in combination_code_review:
                diagnosis_type = item.get('type', '').upper()
                if diagnosis_type == 'SDX':
                    llm_sdx_diagnoses.append(item)
        
        # Count CC/MCC in LLM extracted SDX
        llm_cc_count = 0
        llm_mcc_count = 0
        
        for item in llm_sdx_diagnoses:
            cc_mcc = item.get('cc/mcc', '').upper() if item.get('cc/mcc') else ''
            if cc_mcc == 'CC':
                llm_cc_count += 1
            elif cc_mcc == 'MCC':
                llm_mcc_count += 1
        
        logger.info(f"CC/MCC stats for account {account_num}: GT CC={gt_cc_count}, GT MCC={gt_mcc_count}, LLM CC={llm_cc_count}, LLM MCC={llm_mcc_count}")
        
        return {
            'gt_cc': gt_cc_count,
            'gt_mcc': gt_mcc_count,
            'llm_cc': llm_cc_count,
            'llm_mcc': llm_mcc_count
        }
    except Exception as e:
        logger.error(f"Error calculating CC/MCC statistics for account {account_num}: {e}")
        return {
            'gt_cc': 0,
            'gt_mcc': 0,
            'llm_cc': 0,
            'llm_mcc': 0
        }

def create_cc_mcc_comparison_data(account_num, parsed_data, csv_data):
    """Create detailed CC/MCC comparison data for the table view"""
    try:
        comparison_data = []
        
        # Get ground truth SDX data with CC/MCC information
        ground_truth_sdx = csv_data.get("sdx", {}).get(account_num, [])
        
        # Get LLM extracted SDX data with CC/MCC information
        llm_sdx_diagnoses = []
        if account_num in parsed_data:
            combination_code_review = parsed_data[account_num].get('combination_code_review', [])
            for item in combination_code_review:
                diagnosis_type = item.get('type', '').upper()
                if diagnosis_type == 'SDX':
                    llm_sdx_diagnoses.append(item)
        
        # Create mapping of LLM ICD codes to their CC/MCC values
        llm_icd_mapping = {}
        for item in llm_sdx_diagnoses:
            icd_code = item.get('icd-10-cm code', '')
            cc_mcc = item.get('cc/mcc', '')
            if icd_code:
                llm_icd_mapping[icd_code] = cc_mcc
        
        # Process each ground truth SDX code - only include those with CC/MCC values
        for gt_item in ground_truth_sdx:
            gt_icd = gt_item.get('icd_code', '')
            # Try different possible column names for CC/MCC
            gt_cc_mcc = ''
            if 'cc_mcc' in gt_item:
                gt_cc_mcc = str(gt_item['cc_mcc']) if gt_item['cc_mcc'] else ''
            elif 'ccmcc' in gt_item:
                gt_cc_mcc = str(gt_item['ccmcc']) if gt_item['ccmcc'] else ''
            
            # Only include this item if it has a CC/MCC value
            if gt_cc_mcc and gt_cc_mcc.upper() in ['CC', 'MCC']:
                # Find matching LLM code
                llm_icd = None
                llm_cc_mcc = None
                match_type = 'Mismatch'
                
                if gt_icd in llm_icd_mapping:
                    llm_icd = gt_icd
                    llm_cc_mcc = llm_icd_mapping[gt_icd]
                    match_type = 'Match'
                else:
                    # Check for specificity issues (partial matches)
                    for llm_code in llm_icd_mapping.keys():
                        if len(gt_icd) > 1 and len(llm_code) > 1:
                            # Check if removing last character makes them match
                            if gt_icd[:-1] == llm_code[:-1]:
                                llm_icd = llm_code
                                llm_cc_mcc = llm_icd_mapping[llm_code]
                                match_type = 'Specificity Issue'
                                break
                            # Check if one is a prefix of the other
                            elif gt_icd.startswith(llm_code) or llm_code.startswith(gt_icd):
                                llm_icd = llm_code
                                llm_cc_mcc = llm_icd_mapping[llm_code]
                                match_type = 'Specificity Issue'
                                break
                
                comparison_data.append({
                    'gt_icd': gt_icd,
                    'gt_cc_mcc': gt_cc_mcc,
                    'llm_icd': llm_icd,
                    'llm_cc_mcc': llm_cc_mcc,
                    'match_type': match_type
                })
        
        return comparison_data
    except Exception as e:
        logger.error(f"Error creating CC/MCC comparison data for account {account_num}: {e}")
        return []

def check_sdx_match(icd_code, account_num, csv_data):
    """Check if an ICD code matches with SDX ground truth for the account"""
    try:
        ground_truth_sdx = csv_data.get("sdx", {}).get(account_num, [])
        ground_truth_codes = [item.get('icd_code', '') for item in ground_truth_sdx]
        
        if not icd_code or not ground_truth_codes:
            return "SDX Mismatch"
        
        # Direct match
        if icd_code in ground_truth_codes:
            return "SDX Match"
        
        # Check for specificity issues (partial matches)
        # Remove last character and check if it matches
        for gt_code in ground_truth_codes:
            if len(icd_code) > 1 and len(gt_code) > 1:
                # Check if removing last character makes them match
                if icd_code[:-1] == gt_code[:-1]:
                    return "Specificity Issue"
                # Check if one is a prefix of the other
                if icd_code.startswith(gt_code) or gt_code.startswith(icd_code):
                    return "Specificity Issue"
        
        return "SDX Mismatch"
    except Exception as e:
        logger.error(f"Error checking SDX match for {icd_code}: {e}")
        return "SDX Mismatch"

# Load all data from Excel files
logger.info("Starting data loading process...")
AUDITED_ACCOUNTS_DATA = load_audited_accounts_data()
logger.info(f"Loaded audited accounts data: {len(AUDITED_ACCOUNTS_DATA)} accounts")

DOCUMENTS_DATA = load_documents_from_excel()
logger.info(f"Loaded documents data: {len(DOCUMENTS_DATA)} accounts")

ICD_REVIEW_DATA = load_icd_review_data_from_parsed()
logger.info(f"Loaded ICD review data: {len(ICD_REVIEW_DATA)} accounts")

PARSED_RESULTS_DATA = load_parsed_results_data()
logger.info(f"Loaded parsed results data: {len(PARSED_RESULTS_DATA)} accounts")

CSV_DATA = load_csv_data()
logger.info(f"Loaded CSV data: {len(CSV_DATA.get('pdx', {}))} PDX accounts, {len(CSV_DATA.get('sdx', {}))} SDX accounts")

# Load and merge comparison data
COMPARISON_DATA_RAW = load_coded_vs_llm_comparison()
logger.info(f"Loaded comparison data: {len(COMPARISON_DATA_RAW)} accounts")

COMPARISON_DATA = merge_comparison_with_parsed_data(COMPARISON_DATA_RAW, PARSED_RESULTS_DATA)
logger.info(f"Merged comparison data: {len(COMPARISON_DATA)} accounts")

# Store feedback data
feedback_data = {}

# Store completed accounts
completed_accounts = set()

def load_completed_accounts():
    """Load completed accounts from feedback data"""
    try:
        feedback_file = os.path.join(DATA_DIR, 'feedback_data.xlsx')
        if os.path.exists(feedback_file):
            df = pd.read_excel(feedback_file)
            if not df.empty:
                # Get unique account numbers that have feedback - ensure they are strings
                account_numbers = df['account_number'].dropna().unique()
                for acct_num in account_numbers:
                    try:
                        # Convert to string and ensure it's not a list
                        if isinstance(acct_num, list):
                            logger.warning(f"Skipping list account number: {acct_num}")
                            continue
                        acct_str = str(acct_num).strip()
                        if acct_str:  # Only add non-empty strings
                            completed_accounts.add(acct_str)
                    except Exception as e:
                        logger.warning(f"Error processing account number {acct_num}: {e}")
                        continue
                logger.info(f"Loaded {len(completed_accounts)} completed accounts from feedback data")
    except Exception as e:
        logger.error(f"Error loading completed accounts: {e}")

# Load completed accounts on startup BEFORE creating SAMPLE_ACCOUNTS
load_completed_accounts()

# Store pending accounts - only include accounts that actually exist
pending_accounts = set()

# Create SAMPLE_ACCOUNTS from parsed results data
SAMPLE_ACCOUNTS = {}
logger.info("Creating SAMPLE_ACCOUNTS from parsed results data...")

if PARSED_RESULTS_DATA:
    logger.info(f"Processing {len(PARSED_RESULTS_DATA)} accounts from parsed results data")
    # Use actual account numbers from parsed results data
    for account_num, parsed_data in PARSED_RESULTS_DATA.items():
        logger.info(f"Processing account {account_num}")
        # Calculate statistics dynamically from actual data
        stats = calculate_account_statistics(account_num, PARSED_RESULTS_DATA, CSV_DATA, COMPARISON_DATA)
        logger.info(f"Account {account_num} stats: {stats}")
        
        # Create CC/MCC comparison data for the table view
        cc_mcc_comparison = create_cc_mcc_comparison_data(account_num, PARSED_RESULTS_DATA, CSV_DATA)
        logger.info(f"Account {account_num} CC/MCC comparison: {len(cc_mcc_comparison)} items")
        
        # Get audited account data if available
        audited_data = AUDITED_ACCOUNTS_DATA.get(account_num, {})
        
        # Get documents data for this account - only use real data, no fallback
        documents = DOCUMENTS_DATA.get(account_num, {})
        
        # If no real document data exists, skip this account
        if not documents:
            logger.warning(f"No document data found for account {account_num} - skipping")
            continue
        
        # Only create account if we have real data
        # Ensure proper type conversion for comparison
        total_sdx = int(stats['total_sdx']) if isinstance(stats['total_sdx'], (int, str)) and str(stats['total_sdx']).isdigit() else 0
        pdx_coded = stats['pdx_coded']
        pdx_coded_count = 0 if pdx_coded == 'N/A' or not pdx_coded else 1  # Count as 1 if we have any PDX data
        
        # Check if any SDX codes have CC/MCC tags
        has_cc_mcc_tags = False
        if account_num in CSV_DATA.get("sdx", {}):
            for sdx_item in CSV_DATA["sdx"][account_num]:
                cc_mcc = sdx_item.get('cc_mcc', '') or sdx_item.get('ccmcc', '')
                if cc_mcc and cc_mcc.upper() in ['CC', 'MCC']:
                    has_cc_mcc_tags = True
                    break
        
        # Check for codes that appear in both A. Diagnoses and Comb table
        diagnosis_comb_overlap = []
        if account_num in parsed_data:
            # Get all ICD codes from A. Diagnoses table
            diagnosis_codes = set()
            for diagnosis in parsed_data.get('coded_diagnoses', []):
                icd_code = diagnosis.get('ICD-10-CM Code', '')
                if icd_code:
                    diagnosis_codes.add(icd_code)
            
            # Check which comb codes overlap with diagnosis codes
            for comb_item in parsed_data.get('combination_code_review', []):
                comb_icd = comb_item.get('icd-10-cm code', '')
                if comb_icd in diagnosis_codes:
                    diagnosis_comb_overlap.append(comb_icd)
        
        if documents and (total_sdx > 0 or pdx_coded_count > 0):
            SAMPLE_ACCOUNTS[account_num] = {
                "id": account_num,
                "account_id": account_num,
                "patient_name": f"Account {account_num}",  # Use account number as identifier
                "total_sdx": stats['total_sdx'],
                "llm_matched_sdx": stats['llm_matched_sdx'],
                "matched_icds": stats['matched_icds'],
                "accuracy": stats['accuracy'],
                "pdx_coded": stats['pdx_coded'],
                "pdx_llm": stats['pdx_llm'],
                "pdx_matched": stats['pdx_matched'],
                "cc_mcc_stats": stats['cc_mcc_stats'],
                "cc_mcc_comparison": cc_mcc_comparison,  # Add CC/MCC comparison data
                "has_cc_mcc_tags": has_cc_mcc_tags,  # Flag for CC/MCC tags
                "diagnosis_comb_overlap": diagnosis_comb_overlap,  # Codes that appear in both tables
                "reviewed": account_num in completed_accounts,  # Add reviewed status
                "documents": documents,  # Use the documents data
                "review_items": ICD_REVIEW_DATA.get(account_num, []),
                "comparison_data": COMPARISON_DATA.get(account_num, []),
                "parsed_data": parsed_data,  # Use the actual parsed data
                "csv_data": {
                    "pdx": CSV_DATA.get("pdx", {}).get(account_num, []),
                    "sdx": CSV_DATA.get("sdx", {}).get(account_num, [])
                },
                "audited_data": audited_data  # Include the audited account data if available
            }
            logger.info(f"Created account {account_num} with {len(SAMPLE_ACCOUNTS[account_num]['review_items'])} review items")
        else:
            logger.warning(f"Skipping account {account_num} - insufficient real data (documents: {bool(documents)}, stats: {stats})")
    
    logger.info(f"Created {len(SAMPLE_ACCOUNTS)} accounts from parsed results data")
    
    # Update pending accounts to exclude completed ones
    pending_accounts = set(SAMPLE_ACCOUNTS.keys()) - completed_accounts
    logger.info(f"Updated pending accounts: {len(pending_accounts)} pending, {len(completed_accounts)} completed")
else:
    logger.warning("No parsed results data found - dashboard will be empty")
    # Don't create any sample data - keep SAMPLE_ACCOUNTS empty

# Global variables for time tracking
audit_sessions = {}  # Store session start times: {account_id: {user: start_time}}

def load_audit_times():
    """Load existing audit times from Excel file"""
    try:
        if os.path.exists('audit_times.xlsx'):
            return pd.read_excel('audit_times.xlsx')
        else:
            # Create new file with headers
            df = pd.DataFrame(columns=['account_number', 'auditor', 'start_time', 'end_time', 'duration_minutes', 'date'])
            df.to_excel('audit_times.xlsx', index=False)
            return df
    except Exception as e:
        logger.error(f"Error loading audit times: {e}")
        return pd.DataFrame(columns=['account_number', 'auditor', 'start_time', 'end_time', 'duration_minutes', 'date'])

def save_audit_time(account_number, auditor, start_time, end_time, duration_minutes):
    """Save audit time data to Excel file"""
    try:
        df = load_audit_times()
        new_row = {
            'account_number': account_number,
            'auditor': auditor,
            'start_time': start_time,
            'end_time': end_time,
            'duration_minutes': duration_minutes,
            'date': datetime.now().strftime('%Y-%m-%d')
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_excel('audit_times.xlsx', index=False)
        logger.info(f"Saved audit time for account {account_number} by {auditor}: {duration_minutes:.2f} minutes")
    except Exception as e:
        logger.error(f"Error saving audit time: {e}")

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handle user login"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username in AUDITOR_CREDENTIALS and AUDITOR_CREDENTIALS[username]['password'] == password:
            session['user_id'] = username
            session['user_name'] = AUDITOR_CREDENTIALS[username]['name']
            session['user_team'] = AUDITOR_CREDENTIALS[username]['team']
            logger.info(f"User {username} logged in successfully")
            flash('Login successful!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Invalid username or password', 'error')
            logger.warning(f"Failed login attempt for username: {username}")
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Handle user logout"""
    if 'user_id' in session:
        username = session['user_id']
        session.clear()
        logger.info(f"User {username} logged out")
        flash('You have been logged out', 'info')
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    """Main dashboard page - show only pending accounts"""
    logger.info("Accessing main dashboard")
    
    # Filter only pending accounts
    pending_accounts_data = {k: v for k, v in SAMPLE_ACCOUNTS.items() if k in pending_accounts}
    
    if not SAMPLE_ACCOUNTS:
        logger.warning("No accounts available - showing empty dashboard")
        return render_template('index.html', accounts={}, 
                             total_accounts=0,
                             pending_count=0,
                             completed_count=0,
                             no_data=True)
    
    return render_template('index.html', accounts=pending_accounts_data, 
                         total_accounts=len(SAMPLE_ACCOUNTS),
                         pending_count=len(pending_accounts),
                         completed_count=len(completed_accounts),
                         no_data=False)

@app.route('/review/<account_id>')
@login_required
def review(account_id):
    """Display the review page for a specific account"""
    account = SAMPLE_ACCOUNTS.get(account_id)
    if account:
        # Check if account is already completed - redirect to lookup
        if account_id in completed_accounts:
            logger.info(f"Account {account_id} is completed, redirecting to lookup")
            return redirect(url_for('lookup_account', account_id=account_id))
        
        # Start time tracking for this session
        session['current_audit_account'] = account_id
        session['audit_start_time'] = datetime.now().isoformat()
        logger.info(f"Started audit session for account {account_id} at {session['audit_start_time']}")

        # Pass the entire account object to the template
        return render_template('review.html', account=account)
    return "Account not found", 404

@app.route('/submit_feedback/<account_id>', methods=['POST'])
@login_required
def submit_feedback(account_id):
    """Handle feedback submission"""
    if account_id not in SAMPLE_ACCOUNTS.keys():
        return "Account not found", 404

    # Check if feedback already exists for this account
    feedback_file = os.path.join(DATA_DIR, 'feedback_data.xlsx')
    if os.path.exists(feedback_file):
        try:
            df = pd.read_excel(feedback_file)
            existing_feedback = df[df['account_number'].astype(str) == str(account_id)]
            if not existing_feedback.empty:
                logger.warning(f"Feedback already exists for account {account_id}")
                return jsonify({
                    'status': 'error', 
                    'message': f'Feedback already exists for account {account_id}. Duplicate submissions are not allowed.'
                }), 400
        except Exception as e:
            logger.error(f"Error checking existing feedback: {e}")

    try:
        data = request.form
        feedback_items = []

        # Load parsed data to get CC/MCC information
        parsed_data = load_parsed_results_data()
        account_parsed_data = parsed_data.get(account_id, {})
        combination_code_review = account_parsed_data.get('combination_code_review', [])
        
        # Create a mapping of ICD codes to CC/MCC values
        icd_cc_mcc_mapping = {}
        for item in combination_code_review:
            icd_code = item.get('icd-10-cm code', '')
            cc_mcc = item.get('cc/mcc', '')
            if icd_code:
                icd_cc_mcc_mapping[icd_code] = cc_mcc

        # Process each review item individually
        for key, value in data.items():
            if key.startswith('status_'):
                # Extract ICD code and index from the key (format: status_ICDCODE_INDEX)
                parts = key.replace('status_', '').split('_')
                if len(parts) >= 2:
                    icd_code = parts[0]
                    index = parts[1]
                    feedback_key = f'feedback_{icd_code}_{index}'
                else:
                    # Fallback for old format
                    icd_code = key.replace('status_', '')
                    feedback_key = f'feedback_{icd_code}'
                
                # Get the feedback for this specific ICD code and index
                feedback_text = data.get(feedback_key, '')
                
                # Get CC/MCC information for this ICD code
                cc_mcc = icd_cc_mcc_mapping.get(icd_code, '')
                
                feedback_items.append({
                    'icd_code': icd_code,
                    'status': value,
                    'feedback': feedback_text,
                    'cc_mcc': cc_mcc
                })
        
        # Log the feedback items for debugging
        logger.info(f"Processing {len(feedback_items)} feedback items for account {account_id}")
        for item in feedback_items:
            logger.info(f"ICD: {item['icd_code']}, Status: {item['status']}, CC/MCC: {item['cc_mcc']}, Feedback: {item['feedback'][:50]}...")
        
        feedback_data = {
            'pdx_feedback': data.get('pdx_feedback', ''),
            'adx_feedback': data.get('adx_feedback', ''),
            'sdx_feedback': data.get('sdx_feedback', ''),
            'icd_feedback': feedback_items,
            'reviewer': session.get('user_name', 'Unknown Auditor') 
        }

        save_feedback_to_excel(account_id, feedback_data)

        # End time tracking for this session
        if 'current_audit_account' in session and 'audit_start_time' in session:
            current_account_id = session['current_audit_account']
            start_time_str = session['audit_start_time']
            end_time_str = datetime.now().isoformat()
            
            # Calculate duration in minutes
            start_dt = datetime.fromisoformat(start_time_str)
            end_dt = datetime.fromisoformat(end_time_str)
            duration_minutes = (end_dt - start_dt).total_seconds() / 60
            
            save_audit_time(current_account_id, session['user_name'], start_time_str, end_time_str, duration_minutes)
            logger.info(f"Audit session for account {current_account_id} ended at {end_time_str} with duration {duration_minutes:.2f} minutes")

        # Update account status
        pending_accounts.discard(account_id)
        completed_accounts.add(account_id)

        return jsonify({'status': 'success', 'message': 'Feedback submitted successfully!', 'feedback': feedback_items})

    except Exception as e:
        logger.error(f"Error submitting feedback for account {account_id}: {e}")
        return jsonify({'status': 'error', 'message': 'An error occurred.'}), 500

@app.route('/api/get_random_unreviewed')
def get_random_unreviewed():
    """Get a random unreviewed account"""
    import random
    account_ids = list(SAMPLE_ACCOUNTS.keys())
    
    if not account_ids:
        logger.warning("No accounts available for random selection")
        return jsonify({
            'error': 'No accounts available',
            'message': 'No accounts are currently available for review. Please ensure data files are loaded.'
        }), 404
    
    random_account = random.choice(account_ids)
    
    logger.info(f"Random unreviewed account selected: {random_account}")
    return jsonify({
        'account_id': random_account,
        'redirect_url': url_for('review', account_id=random_account)
    })

@app.route('/api/account/<account_id>')
def get_account_data(account_id):
    """Get account data via API"""
    if account_id in SAMPLE_ACCOUNTS:
        return jsonify(SAMPLE_ACCOUNTS[account_id])
    else:
        return jsonify({'error': 'Account not found'}), 404

@app.route("/account_history/<account_id>")
def account_history(account_id):
    """Display the feedback history for a specific account"""
    account = SAMPLE_ACCOUNTS.get(account_id)
    if not account:
        return "Account not found", 404

    feedback_records = []
    general_feedback = {}
    if os.path.exists(os.path.join(DATA_DIR, 'feedback_data.xlsx')):
        try:
            all_feedback_df = pd.read_excel(os.path.join(DATA_DIR, 'feedback_data.xlsx'), dtype={'account_number': str})
            account_feedback_df = all_feedback_df[all_feedback_df['account_number'] == account_id]

            if not account_feedback_df.empty:
                # Extract general feedback (PDX, ADX, SDX) - it's stored with each ICD row
                # We can just take it from the first row for this account
                first_row = account_feedback_df.iloc[0]
                general_feedback = {
                    'pdx_feedback': first_row.get('pdx_feedback', ''),
                    'adx_feedback': first_row.get('adx_feedback', ''),
                    'sdx_feedback': first_row.get('sdx_feedback', '')
                }

            feedback_records = account_feedback_df.to_dict('records')
        except Exception as e:
            logger.error(f"Error reading or processing feedback_data.xlsx: {e}")

    return render_template('account_history.html', 
                           account=account, 
                           feedback_data=feedback_records,
                           general_feedback=general_feedback)


@app.route('/api/account_feedback/<account_id>')
def get_account_feedback(account_id):
    """Get feedback data for a specific account from Excel"""
    try:
        if os.path.exists(os.path.join(DATA_DIR, 'feedback_data.xlsx')):
            df = pd.read_excel(os.path.join(DATA_DIR, 'feedback_data.xlsx'))
            # Filter for the specific account and remove empty rows
            # Handle both string and numeric account numbers
            account_feedback = df[df['account_number'].astype(str) == str(account_id)].dropna(subset=['icd_code'])
            
            # Convert to list of dictionaries
            feedback_list = []
            for _, row in account_feedback.iterrows():
                feedback_list.append({
                    'icd_code': str(row['icd_code']),
                    'accept_reject': str(row['accept_reject']),
                    'feedback_comment': str(row['feedback_comment']) if pd.notna(row['feedback_comment']) else '',
                    'reviewer': str(row['reviewer']),
                    'timestamp': str(row['timestamp'])
                })
            
            return jsonify(feedback_list)
        else:
            return jsonify([])
    except Exception as e:
        logger.error(f"Error loading feedback for account {account_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/validation')
@login_required
def validation_dashboard():
    """Display the validation dashboard with stats from feedback data."""
    feedback_file = os.path.join(DATA_DIR, 'feedback_data.xlsx')
    stats = {
        'total_feedback': 0,
        'accepted_codes': 0,
        'rejected_codes': 0,
        'acceptance_rate': 0,
        'total_accounts_reviewed': 0,
        'latest_feedback_timestamp': 'No feedback yet',
        'average_response_time': 'No data available'
    }
    all_feedback = []

    if os.path.exists(feedback_file):
        try:
            df = pd.read_excel(feedback_file)
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                stats['total_feedback'] = len(df)
                stats['accepted_codes'] = len(df[df['accept_reject'] == 'accept'])
                stats['rejected_codes'] = len(df[df['accept_reject'] == 'reject'])
                
                if stats['total_feedback'] > 0:
                    stats['acceptance_rate'] = round((stats['accepted_codes'] / stats['total_feedback']) * 100)
                
                stats['total_accounts_reviewed'] = df['account_number'].nunique()
                
                latest_feedback = df.sort_values('timestamp', ascending=False).iloc[0]
                stats['latest_feedback_timestamp'] = latest_feedback['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

                # Process feedback data - each row is already an individual ICD feedback entry
                all_feedback = []
                for _, row in df.sort_values('timestamp', ascending=False).iterrows():
                    try:
                        # Each row represents one ICD feedback entry
                        feedback_entry = {
                            'account_number': row['account_number'],
                            'icd_code': row.get('icd_code', 'Unknown'),
                            'decision': row.get('accept_reject', 'unknown'),
                            'feedback': row.get('feedback_comment', 'No feedback provided'),
                            'reviewer': row.get('reviewer', 'Unknown'),
                            'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                            'cc_mcc': row.get('cc_mcc', ''),
                            'type': 'ICD Feedback'
                        }
                        all_feedback.append(feedback_entry)
                            
                    except Exception as e:
                        logger.error(f"Error processing feedback row: {e}")
                        # Add fallback entry
                        feedback_entry = {
                            'account_number': row.get('account_number', 'Unknown'),
                            'icd_code': 'Error',
                            'decision': 'error',
                            'feedback': f'Error processing feedback: {str(e)}',
                            'reviewer': row.get('reviewer', 'Unknown'),
                            'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                            'cc_mcc': '',
                            'type': 'ICD Feedback'
                        }
                        all_feedback.append(feedback_entry)

        except Exception as e:
            logger.error(f"Error processing validation dashboard data: {e}")
    
    # Calculate actual average response time from audit times data
    try:
        audit_times_df = load_audit_times()
        if not audit_times_df.empty:
            avg_duration = audit_times_df['duration_minutes'].mean()
            stats['average_response_time'] = f"{avg_duration:.1f} minutes"
        else:
            stats['average_response_time'] = 'No audit data available'
    except Exception as e:
        logger.error(f"Error calculating average response time: {e}")
        stats['average_response_time'] = 'Error calculating'
    
    return render_template('validation.html', stats=stats, all_feedback=all_feedback)


@app.route('/audit-times')
@admin_required
def audit_times():
    """Display audit time tracking data"""
    try:
        # Load audit times data
        audit_times_df = load_audit_times()
        
        if audit_times_df.empty:
            return render_template('audit_times.html', audit_times=[], stats={})
        
        # Calculate statistics
        total_audits = len(audit_times_df)
        avg_duration = audit_times_df['duration_minutes'].mean() if total_audits > 0 else 0
        min_duration = audit_times_df['duration_minutes'].min() if total_audits > 0 else 0
        max_duration = audit_times_df['duration_minutes'].max() if total_audits > 0 else 0
        
        # Get auditor performance - properly format the data
        auditor_stats = []
        if not audit_times_df.empty:
            auditor_groups = audit_times_df.groupby('auditor')
            for auditor_name, group in auditor_groups:
                auditor_stat = {
                    'auditor': auditor_name,
                    'total_audits': len(group),
                    'avg_duration': round(group['duration_minutes'].mean(), 2),
                    'min_duration': round(group['duration_minutes'].min(), 2),
                    'max_duration': round(group['duration_minutes'].max(), 2)
                }
                auditor_stats.append(auditor_stat)
        
        stats = {
            'total_audits': total_audits,
            'avg_duration': round(avg_duration, 2),
            'min_duration': round(min_duration, 2),
            'max_duration': round(max_duration, 2),
            'auditor_stats': auditor_stats
        }
        
        # Convert to list of dictionaries for template
        audit_times = []
        for _, row in audit_times_df.iterrows():
            audit_item = {
                'account_number': row['account_number'],
                'auditor': row['auditor'],
                'start_time': row['start_time'],
                'end_time': row['end_time'],
                'duration_minutes': round(row['duration_minutes'], 2),
                'date': row['date']
            }
            audit_times.append(audit_item)
        
        # Sort by date (newest first)
        audit_times.sort(key=lambda x: x['date'], reverse=True)
        
        return render_template('audit_times.html', audit_times=audit_times, stats=stats)
        
    except Exception as e:
        logger.error(f"Error in audit_times route: {e}")
        flash(f"Error loading audit times data: {str(e)}", 'error')
        return render_template('audit_times.html', audit_times=[], stats={})


@app.route('/completed')
@login_required
def completed_accounts_page():
    """Display the list of completed accounts"""
    logger.info("Accessing completed accounts page")
    
    # Filter only completed accounts
    completed_accounts_data = {k: v for k, v in SAMPLE_ACCOUNTS.items() if k in completed_accounts}
    
    return render_template('completed.html', accounts=completed_accounts_data,
                         completed_count=len(completed_accounts),
                         total_accounts=len(SAMPLE_ACCOUNTS))

@app.route('/lookup/<account_id>')
@login_required
def lookup_account(account_id):
    """Display completed account for lookup only - no modifications allowed"""
    logger.info(f"Accessing lookup page for account {account_id}")
    
    if account_id not in SAMPLE_ACCOUNTS:
        return "Account not found", 404
    
    account = SAMPLE_ACCOUNTS[account_id]
    
    # Allow lookup for any account that has feedback data
    return render_template('lookup.html', account=account)

@app.route('/move_to_queue/<account_id>', methods=['POST'])
@login_required
def move_to_queue(account_id):
    """Move a completed account back to the review queue by removing all coded data"""
    logger.info(f"Moving account {account_id} back to review queue")
    
    try:
        # Remove from completed accounts
        if account_id in completed_accounts:
            completed_accounts.remove(account_id)
            logger.info(f"Removed account {account_id} from completed accounts")
        
        # Add back to pending accounts
        if account_id in SAMPLE_ACCOUNTS:
            pending_accounts.add(account_id)
            logger.info(f"Added account {account_id} back to pending accounts")
        
        # Remove all feedback data for this account
        feedback_file = os.path.join(DATA_DIR, 'feedback_data.xlsx')
        if os.path.exists(feedback_file):
            df = pd.read_excel(feedback_file)
            # Remove all rows for this account
            df = df[df['account_number'].astype(str) != str(account_id)]
            df.to_excel(feedback_file, index=False)
            logger.info(f"Removed all feedback data for account {account_id}")
        
        return jsonify({'status': 'success', 'message': f'Account {account_id} moved back to review queue'})
    except Exception as e:
        logger.error(f"Error moving account {account_id} to queue: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('static', exist_ok=True)
    os.makedirs('templates', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    app.run(debug=True, host='0.0.0.0', port=9999) 
