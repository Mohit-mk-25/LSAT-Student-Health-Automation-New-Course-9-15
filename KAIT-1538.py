import pandas as pd
pd.set_option('display.max_columns', None) 
import numpy as np
import json
import shutil
from datetime import datetime, timedelta
import pygsheets
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings("ignore")
from common_utils import compose_email, connect_to_db, tableauLogger
def read_from_gsheets(gc,sheet_id, tab_name):

    _sheet = gc.open_by_key(sheet_id)
    _wksheet = _sheet.worksheet('title', tab_name)
    _df = _wksheet.get_as_df()

    return _df
def load_to_gsheets(gc, df_in, sheet_id, tab_name):

    sheet = gc.open_by_key(sheet_id)
    worksheet = sheet.worksheet('title', tab_name)
    worksheet.clear()
    worksheet.set_dataframe(df_in, (1, 1), fit=True, nan='')

def get_prod_codes(gc,sheet_id , sheet_name):
    prod_code_df = read_from_gsheets(
        gc, sheet_id , sheet_name
    )
    return prod_code_df

def prepare_ESD_start_end_range(gc):
    ESD_range = read_from_gsheets(gc,'1A8_B7dq3EO30vxJmGW6vn4D6g-fbhgqii4ufSo25FX0','ESD_Range')
    ESD_range_start = ESD_range['Enroll Start Date (start)'][0]
    ESD_range_end = ESD_range['Enroll Start Date (end)'][0]
    
    ESD_range_start = pd.to_datetime(ESD_range_start)

    if not ESD_range_end:
        end_date = ESD_range_start + relativedelta(years=5)
    else:
        end_date = pd.to_datetime(ESD_range_end)

    start_str = ESD_range_start.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    start_str = ''.join(f"'{start_str}'")
    end_str = ''.join(f"'{end_str}'")
    return start_str, end_str
def load_query(gc, file_path,lol_prod_code, tut_prod_code):
    with open(file_path, 'r') as fp:
        raw_q = ''.join(fp.readlines())

    wilder_query = raw_q.replace('%%', '^^').replace('%', '%%').replace('^^', '%%')

    formatted_lol_codes = ', '.join(f"'{code}'" for code in lol_prod_code)
    formatted_tut_codes = ', '.join(f"'{code}'" for code in tut_prod_code)

    # Default Upper Limit
    start , end  = prepare_ESD_start_end_range(gc)
    
    formatted_query = wilder_query.format(
        lol_product_placeholder=formatted_lol_codes
        ,tut_product_placeholder=formatted_tut_codes
        , ESD_placeholder_start = start
        , ESD_placeholder_end = end
    )
    return formatted_query

def get_data_prod_code(data, prod_code):
    return data[data['tut_product_code'].apply(lambda codes: any(code in codes for code in prod_code))]
def get_assign_name(gc):
    assign_df = read_from_gsheets(gc,'1A8_B7dq3EO30vxJmGW6vn4D6g-fbhgqii4ufSo25FX0','Activity list + lookup')
    return assign_df
def aggregate_ordered_unique(group):
    unique_enrollments = list(dict.fromkeys(group['kbs_enrollment_id']))
    unique_products = list(dict.fromkeys(group['product_code']))
    unique_cl_code = list(dict.fromkeys(
        [x for x in group['class_code'] if pd.notna(x)]
    ))

    
    return pd.Series({
        'First Name': group['person_first_name'].iloc[0],
        'Last Name': group['person_last_name'].iloc[0],
        'Email': group['person_email'].iloc[0],
        'kbs_enrollment_id': unique_enrollments,
        'Product code': unique_products,
        'Class code': unique_cl_code,
        'Class start date': group['class_start_date'].min(),
        '# Enrollments': len(unique_enrollments),
        '# Products': len(unique_products),
        'enroll_start_date': group['enroll_start_date'].min(),
        'enroll_exp_date': group['enroll_exp_date'].max()
        
    })

def get_req_data(act_df, tut_prod_code, lol_prod_code):
    tut_prod_df = act_df[act_df["product_code"].isin(tut_prod_code)]
    lol_prod_df = act_df[act_df["product_code"].isin(lol_prod_code)]
    common_df = lol_prod_df[lol_prod_df["person_student_id"].isin(tut_prod_df['person_student_id'].unique())]
    data_all_req = pd.concat([tut_prod_df , common_df])
    return data_all_req , tut_prod_df

def get_eligible_student(act_df, tut_prod_code, lol_prod_code):
    student_details_data , tut_df = get_req_data(act_df, tut_prod_code, lol_prod_code)
    student_details_data = student_details_data.sort_values(by=['person_student_id', 'enroll_start_date'])
    student_details = (
    student_details_data
        .groupby('person_student_id')
        .apply(aggregate_ordered_unique)
        .reset_index()
    )
    student_details['person_student_id'] = student_details['person_student_id'].astype(int)
    
    return student_details

def get_act_report(gc,act_df,lol_prod_code, tut_prod_code):
    cleaned_df_raw , tut_prod_df = get_req_data(act_df, tut_prod_code, lol_prod_code)
    assign_name = get_assign_name(gc)
    cleaned_df_raw = pd.merge(cleaned_df_raw , assign_name[['Type for Dashboard','Assignment Name']] , left_on='sequence_name' , right_on='Assignment Name' , how = 'left')
    cleaned_df = cleaned_df_raw[cleaned_df_raw["date_completed"] >= cleaned_df_raw['enroll_start_date']]
    cleaned_df = cleaned_df.loc[cleaned_df.groupby(['person_student_id','sequence_name'])['date_completed'].idxmin()]

    col_to_check = assign_name['Type for Dashboard'].dropna().unique()
    
    col_map_df = pd.DataFrame(cleaned_df['person_student_id'].unique() , columns = ['person_student_id'])
    for col in col_to_check:
        test_df = cleaned_df[cleaned_df['Type for Dashboard'] == col]
        test_df_gr = test_df.groupby(['person_student_id'])['Type for Dashboard'].count().reset_index()
        col_map_df = pd.merge(col_map_df , test_df_gr , on = 'person_student_id' , how='left')
        col_map_df = col_map_df.rename(columns={'Type for Dashboard':col+" Count"})
        
    col_map_df['person_student_id'] = col_map_df['person_student_id'].astype(int)
    student_details = get_eligible_student(act_df, tut_prod_code, lol_prod_code)
    student_report = pd.merge(student_details ,col_map_df , on='person_student_id',how='left')
    student_report = student_report.fillna('')
    student_report['person_student_id'] = student_report['person_student_id'].astype(int)

    tut_prod_df_gr = tut_prod_df.groupby(['person_student_id']).agg({
            'product_code':'unique',
            'kbs_enrollment_id':'unique'
        }).reset_index()
    tut_prod_df_gr['product_code'] = tut_prod_df_gr['product_code'].apply(list)
    tut_prod_df_gr['kbs_enrollment_id'] = tut_prod_df_gr['kbs_enrollment_id'].apply(list)
    tut_prod_df_gr = tut_prod_df_gr.rename(columns = {
        'product_code':'tut_product_code',
        'kbs_enrollment_id':'tut_kbs_enrollment_id'
    })
    cleaned_df_raw = pd.merge(cleaned_df_raw , tut_prod_df_gr , on='person_student_id' , how='inner')
    
    student_report = student_report.sort_values(by='person_student_id' , ascending=False)
    return cleaned_df_raw , student_report

def get_score_report(gc,act_df,score_df,lol_prod_code,tut_prod_code):
    data_all_req , tut_prod_df = get_req_data(act_df, tut_prod_code, lol_prod_code)
    cleaned_df_raw , student_report = get_act_report(gc,act_df,lol_prod_code, tut_prod_code)
    cleaned_df_score = score_df[score_df["person_student_id"].isin(data_all_req['person_student_id'])]
    cleaned_df_score = cleaned_df_score.loc[cleaned_df_score.groupby(['person_student_id','activity_name'])['date_completed'].idxmin()]

    score_col = {
    'lsac24pt153': 'Diagnostics',
    'lsac24pt154': 'Midpoint 1',
    'lsac24pt152': 'Midpoint 2',
    'lsac24pt155': 'Final'
    }

    score_map_df = pd.DataFrame(cleaned_df_raw['person_student_id'].unique(), columns=['person_student_id'])
    
    for column in score_col.keys():
        label = score_col[column]
        col_df = cleaned_df_score[cleaned_df_score["activity_name"] == column]
        col_df = col_df[['person_student_id','activity_name','date_created','total_items','total_scored_items',
                         'total_scored_items_answered','total_scored_items_answered_correct','score_value']]
        
        col_df.columns = [col_df.columns[0]] + [label + ' ' + col for col in col_df.columns[1:]]
        
        score_map_df = pd.merge(score_map_df, col_df, on='person_student_id', how='left')

    score_map_df['person_student_id'] = score_map_df['person_student_id'].astype(int)

    ## Three digit PT map
    three_digit_pt_df = pd.DataFrame(cleaned_df_raw['person_student_id'].unique(), columns=['person_student_id'])

    filt_3_dig_pt = cleaned_df_score[cleaned_df_score['activity_name'].str.contains(r'lsac.*\d{3}$', case=False, na=False)]
    filt_3_dig_pt_gr = filt_3_dig_pt.groupby('person_student_id').agg({
        'activity_name': 'nunique',
    }).reset_index()
    filt_3_dig_pt_gr = filt_3_dig_pt_gr.rename(columns={
        'activity_name':'Three_Digit_PT_Count'
    })
    three_digit_pt_df = pd.merge(three_digit_pt_df , filt_3_dig_pt_gr , on='person_student_id' , how='left')
    three_digit_pt_df['person_student_id'] = three_digit_pt_df['person_student_id'].astype(int)
        
    first_score_df_date = cleaned_df_score.loc[cleaned_df_score.groupby(['person_student_id'])['date_created'].idxmin()]
    first_score_df_date_col = first_score_df_date[['person_student_id','activity_name','date_created','total_items','total_scored_items',
                                                   'total_scored_items_answered','total_scored_items_answered_correct','score_value']]  
    first_score_df_date_col.columns =[first_score_df_date_col.columns[0]] + ['First_' + col for col in first_score_df_date_col.columns[1:]]
    first_score_df_date_col['person_student_id'] = first_score_df_date_col['person_student_id'].astype(int)
    
    max_score_df_value = cleaned_df_score.loc[cleaned_df_score.groupby(['person_student_id'])['score_value'].idxmax()]
    max_score_df_value = max_score_df_value[['person_student_id','activity_name','date_created','total_items','total_scored_items',
                                                   'total_scored_items_answered','total_scored_items_answered_correct','score_value']]
    max_score_df_value.columns =[max_score_df_value.columns[0]] + ['Max_' + col for col in max_score_df_value.columns[1:]]
    max_score_df_value['person_student_id'] = max_score_df_value['person_student_id'].astype(int)
    
    

    ## Last Lol Class date - Start
    cleaned_df_lol = cleaned_df_raw[cleaned_df_raw['product_code'].isin(lol_prod_code)]
    cleaned_df_lol = cleaned_df_lol[cleaned_df_lol['status'] == 'completed']
    cleaned_df_lol_last_act = cleaned_df_lol.loc[cleaned_df_lol.groupby(['person_student_id'])['date_completed'].idxmin()]
    cleaned_df_lol_last_act = cleaned_df_lol_last_act[['person_student_id' , 'date_completed']]
    cleaned_df_lol_last_act = cleaned_df_lol_last_act.rename(columns={'date_completed':'Last day of LoL class'})
    cleaned_df_lol_last_act['person_student_id'] = cleaned_df_lol_last_act['person_student_id'].astype(int)

    ### End

    ## core lesson completion
    core_lesson_list_df = read_from_gsheets(gc,'1A8_B7dq3EO30vxJmGW6vn4D6g-fbhgqii4ufSo25FX0','List of videos/Perform Quizes and Review Quizes')
    core_lesson_comp_df = pd.DataFrame(cleaned_df_raw['person_student_id'].unique(), columns=['person_student_id'])
    core_lession_video = cleaned_df_raw[cleaned_df_raw['sequence_name'].isin(core_lesson_list_df['Required Vidoes'])].groupby('person_student_id')['sequence_name'].nunique().reset_index(name='# of Videos out of 50')
    core_lession_perf_quiz = cleaned_df_raw[cleaned_df_raw['sequence_name'].isin(core_lesson_list_df['Perform Quizzes'])].groupby('person_student_id')['sequence_name'].nunique().reset_index(name='# of Perform Quiz out of 17')
    core_lession_rev_quiz = cleaned_df_raw[cleaned_df_raw['sequence_name'].isin(core_lesson_list_df['Review Quizzes'])].groupby('person_student_id')['sequence_name'].nunique().reset_index(name='# of Review Quiz out of 9')

    core_lesson_comp_df = pd.merge(core_lesson_comp_df , core_lession_video , on='person_student_id' , how='left')
    core_lesson_comp_df = pd.merge(core_lesson_comp_df , core_lession_perf_quiz , on='person_student_id' , how='left')
    core_lesson_comp_df = pd.merge(core_lesson_comp_df , core_lession_rev_quiz , on='person_student_id' , how='left')
    core_lesson_comp_df['person_student_id'] = core_lesson_comp_df['person_student_id'].astype(int)


    student_report = pd.merge(student_report , cleaned_df_lol_last_act , on='person_student_id',how='left')
    student_report = pd.merge(student_report , three_digit_pt_df , on='person_student_id',how='left')
    student_report = pd.merge(student_report , score_map_df , on='person_student_id',how='left')
    student_report = pd.merge(student_report , first_score_df_date_col , on='person_student_id',how='left')
    student_report = pd.merge(student_report , max_score_df_value , on='person_student_id',how='left')
    student_report = pd.merge(student_report , core_lesson_comp_df , on='person_student_id',how='left')
    
    student_report = student_report.fillna('')
    tut_prod_df = tut_prod_df.sort_values(by=['person_student_id', 'enroll_start_date'])
    tut_prod_df = tut_prod_df.loc[tut_prod_df.groupby(['person_student_id'])['enroll_start_date'].idxmin()]
    tut_prod_df_gr = tut_prod_df.groupby(['person_student_id']).agg({
            'product_code':'unique',
            'kbs_enrollment_id':'unique'
        }).reset_index()
    tut_prod_df_gr['product_code'] = tut_prod_df_gr['product_code'].apply(list)
    tut_prod_df_gr['kbs_enrollment_id'] = tut_prod_df_gr['kbs_enrollment_id'].apply(list)
    tut_prod_df_gr = tut_prod_df_gr.rename(columns = {
        'product_code':'tut_product_code',
        'kbs_enrollment_id':'tut_kbs_enrollment_id'
    })
    tut_prod_df_gr['person_student_id'] = tut_prod_df_gr['person_student_id'].astype(int)
    student_report = pd.merge(student_report , tut_prod_df_gr , on='person_student_id' , how='left')
    student_report = student_report.sort_values(by='person_student_id' , ascending = False)
    return cleaned_df_raw , student_report

def main():
    conn = connect_to_db('redshift_prod')
    report_name = 'LSAT Student Health New Course (9/15)'
    gc = pygsheets.authorize(service_file='ktp-datasci-py-9ceeec0a3552.json')
    now = datetime.now() - timedelta(hours=5)
    file_now = now.strftime('%Y%m%d_%H%M')
    mail_now = now.strftime('%d %b %Y %H:%M')

    recipients = ['anup.pillai@kaplan.com', 'mohit.kumar@kaplan.edu']

    default_msg_body = """KTP Business Intelligence
    business.intelligence@kaplan.com
    Need to request data, or update an existing report? Please fill out our form:
    http://kaplansurvey.com/admin/request/index.php?form=request_bireport
    """

    mail_params = {
        'subject': report_name + ' was executed at ' + mail_now,
        'to': recipients,
    }
    mail_params['body'] = default_msg_body.replace('\n', '<br>')

    generated_files = []

    lol_prod_code_df = get_prod_codes(gc,'1A8_B7dq3EO30vxJmGW6vn4D6g-fbhgqii4ufSo25FX0','LOL_Product')
    lol_prod_code = lol_prod_code_df.loc[lol_prod_code_df['Active'] == 1 , 'Product Code'].dropna().tolist()
    tut_prod_code_df = get_prod_codes(gc,'1A8_B7dq3EO30vxJmGW6vn4D6g-fbhgqii4ufSo25FX0','TuT_Product')
    tut_prod_code = tut_prod_code_df.loc[tut_prod_code_df['Active'] == 1 , 'Product Code'].dropna().tolist()

    act_query  = load_query(gc,'LSAT_activity_query.sql', lol_prod_code, tut_prod_code)
    score_query  = load_query(gc,'lsat_score_main.sql', lol_prod_code, tut_prod_code)
    act_df = pd.read_sql(act_query, conn)
    score_df = pd.read_sql(score_query, conn)

    


    cleaned_df_raw, student_report = get_score_report(gc,act_df,score_df,lol_prod_code,tut_prod_code)
    s1_tab = 'Report_score_Activity'
    s2_tab = 'Activities'

 
    tut_prod_code_df_gsheet = tut_prod_code_df[tut_prod_code_df['Active'] == 1]
    Product_code_sheet_df = (
        tut_prod_code_df_gsheet.groupby('Sheet Id')['Product Code']
        .apply(list)
        .reset_index(name='Product Code')
    )
    prod_codes = Product_code_sheet_df['Product Code'].dropna()
    gsheet_ids = Product_code_sheet_df['Sheet Id'].dropna()

    cleaned_df_raw, student_report = get_score_report(gc,act_df,score_df,lol_prod_code, tut_prod_code)
    
    for p_codes, sheet_id in zip(prod_codes, gsheet_ids):
        filter_df_score = get_data_prod_code(student_report, p_codes)
        filter_df_activity = get_data_prod_code(cleaned_df_raw, p_codes)
        s1_csv_file = f'{report_name}_{p_codes[0]}_{s1_tab}__{file_now}.csv'
        s2_csv_file = f'{report_name}_{p_codes[0]}_{s2_tab}__{file_now}.csv'

        load_to_gsheets(gc, filter_df_score, sheet_id, s1_tab)
        filter_df_score.to_csv(s1_csv_file, index=False)
        generated_files.append(s1_csv_file)

        load_to_gsheets(gc, filter_df_activity, sheet_id, s2_tab)
        filter_df_activity.to_csv(s2_csv_file, index=False)
        generated_files.append(s2_csv_file)

    compose_email(sender='business.intelligence@kaplan.com', **mail_params)
    for gfile in generated_files:
        shutil.move(gfile, 'archive/')

if __name__ == '__main__':

    log_obj = tableauLogger(job=report_name, repo='PYNS')

    main()

    log_obj.close_log()

