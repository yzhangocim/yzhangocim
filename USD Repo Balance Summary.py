### ------------------------------------------------------------------- REPO BALANCE SUMMARY ------------------------------------------------------------------- ###

### PACKAGES ###
import pandas as pd
from datetime import date
import os         
import numpy as np                                                          
from pretty_html_table import build_table
from bs4 import BeautifulSoup

### CLIENTS ###
import scripts.azure_sql_db.sql_client as sc
import scripts.clients.new_enfusion_client as enf

### ------------------------------------------------------------------------ DATA PULL ------------------------------------------------------------------------ ###

### ENFUSION ###
client = enf.EnfusionClient('serviceuser@ocimgt.com', 'Enfusion2023!')
live_repos_positions = client.get_live_repos_today_df()

### ISOLATE COUNTRIES ###
usd_df = live_repos_positions[live_repos_positions['Risk Country'] == 'United States']
usd_df = usd_df[usd_df['First Counterparty Name'] != 'Internal']
usd_df['Haircut Amount'] = ((1 - usd_df['Repurchase Agreement Haircut']) * usd_df['$ NMV']) / 1000000
unique_ctpy_array = usd_df['First Counterparty Name'].unique()
colnames = np.append('Total',unique_ctpy_array)

### TODAY DATE ###
today_date = date.today()
today_date_string = today_date.strftime("%m%d%Y")
today_dt = today_date.strftime("%m/%d/%Y")
today_sql_dt = today_date.strftime("%Y-%m-%d")

### YESTERDAY DATE ###
ts = pd.Timestamp(str(today_date))
offset = pd.tseries.offsets.BusinessDay(n=1)
yester_date = today_date - offset
yester_date_string = yester_date.strftime("%Y%m%d")
yesterday_dt = yester_date.strftime("%m/%d/%Y")
sql_dt = today_date.strftime("%Y-%m-%d")

### IMPORT SQL DATA ###
with sc.SQLClient() as client:
    tjm_repo = client.pd_from_sql("SELECT * from repo_tjm")
    jpm_repo = client.pd_from_sql("SELECT * from repo_jpm")
ytd_tjm = tjm_repo[pd.to_datetime(tjm_repo['date']) == sql_dt]
today_tjm = tjm_repo[pd.to_datetime(tjm_repo['date']) == sql_dt]
ytd_jpm = jpm_repo[pd.to_datetime(jpm_repo['date']) == sql_dt]
today_jpm = jpm_repo[pd.to_datetime(jpm_repo['date']) == sql_dt]

### ------------------------------------------------------------------ REPO BALANCE ANALYSIS ------------------------------------------------------------------ ###

### CREATE DATAFRAME FOR ALL COUNTERPARTIES AND FINAL EXPORT ###
usd_total_balances_df = pd.DataFrame(columns = colnames,
                                     index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
usd_overnight_balances_df = pd.DataFrame(columns = colnames,
                                                    index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
usd_term_balances_df = pd.DataFrame(columns = colnames,
                                    index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
usd_haircut_df = pd.DataFrame(columns = colnames,
                              index = ['Gross Balance','Net Balance','Calculated Haircut Amount','Realized Haircut % of Gross'])

### TOTAL COLUMNS - TOTAL BALANCES###
usd_total_balances_df.loc['Dealer Bids','Total'] = round(usd_df[usd_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
usd_total_balances_df.loc['Dealer Offers','Total'] = round(usd_df[usd_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
usd_total_balances_df.loc['Gross','Total'] = round((usd_total_balances_df.loc['Dealer Bids','Total'] * -1) + usd_total_balances_df.loc['Dealer Offers','Total']) * -1
usd_total_balances_df.loc['Net','Total'] = round(usd_total_balances_df.loc['Dealer Bids','Total'] + usd_total_balances_df.loc['Dealer Offers','Total'])
usd_total_balances_df.loc['Gross %','Total'] =  str(round(usd_total_balances_df.loc['Gross','Total'] / usd_total_balances_df.loc['Gross','Total'] * 100)) + '%'
usd_total_balances_df.loc['Net %','Total'] =  str(round(usd_total_balances_df.loc['Net','Total'] / usd_total_balances_df.loc['Net','Total'] * 100)) + '%'

### HAIRCUT ANALYSIS ###
usd_haircut_df.loc['Gross Balance','Total'] = round(((usd_total_balances_df.loc['Dealer Bids','Total'] * -1) + usd_total_balances_df.loc['Dealer Offers','Total']) * -1)
usd_haircut_df.loc['Net Balance','Total'] = round(usd_total_balances_df.loc['Dealer Bids','Total'] + usd_total_balances_df.loc['Dealer Offers','Total'])
usd_haircut_df.loc['Calculated Haircut Amount','Total'] = round(usd_df['Haircut Amount'].sum() * -1)
usd_haircut_df.loc['Realized Haircut % of Gross','Total'] = str(round(100 * usd_haircut_df.loc['Calculated Haircut Amount','Total'] / usd_haircut_df.loc['Gross Balance','Total'],2)) + '%'

### TOTAL COLUMNS - TOTAL BALANCES###
usd_overnight_df = usd_df[usd_df['Days To Maturity'] == 1]
if(len(usd_overnight_df) != 0):
    usd_overnight_balances_df.loc['Dealer Bids','Total'] = round(usd_overnight_df[usd_overnight_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
    usd_overnight_balances_df.loc['Dealer Offers','Total'] = round(usd_overnight_df[usd_overnight_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
    usd_overnight_balances_df.loc['Gross','Total'] =  round((usd_overnight_balances_df.loc['Dealer Bids','Total'] * -1) + usd_overnight_balances_df.loc['Dealer Offers','Total']) * -1
    usd_overnight_balances_df.loc['Net','Total'] =  round(usd_overnight_balances_df.loc['Dealer Bids','Total'] + usd_overnight_balances_df.loc['Dealer Offers','Total'])
    usd_overnight_balances_df.loc['Gross %','Total'] =  str(round(usd_overnight_balances_df.loc['Gross','Total'] / usd_overnight_balances_df.loc['Gross','Total'] * 100)) + '%'
    usd_overnight_balances_df.loc['Net %','Total'] =  str(round(usd_overnight_balances_df.loc['Net','Total'] / usd_overnight_balances_df.loc['Net','Total'] * 100)) + '%'

### TOTAL COLUMNS - TOTAL BALANCES###
usd_term_df = usd_df[usd_df['Days To Maturity'] > 1]
if(len(usd_term_df) != 0):
    usd_term_balances_df.loc['Dealer Bids','Total'] = round(usd_term_df[usd_term_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
    usd_term_balances_df.loc['Dealer Offers','Total'] = round(usd_term_df[usd_term_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
    usd_term_balances_df.loc['Gross','Total'] =  round((usd_term_balances_df.loc['Dealer Bids','Total'] * -1) + usd_term_balances_df.loc['Dealer Offers','Total']) * -1
    usd_term_balances_df.loc['Net','Total'] =  round(usd_term_balances_df.loc['Dealer Bids','Total'] + usd_term_balances_df.loc['Dealer Offers','Total'])
    usd_term_balances_df.loc['Gross %','Total'] =  str(round(usd_term_balances_df.loc['Gross','Total'] / usd_term_balances_df.loc['Gross','Total'] * 100)) + '%'
    usd_term_balances_df.loc['Net %','Total'] =  str(round(usd_term_balances_df.loc['Net','Total'] / usd_term_balances_df.loc['Net','Total'] * 100)) + '%'

### FOR EACH COUNTERPARTY ###
unique_ctpy = usd_df['First Counterparty Name'].unique()
for ctpy in unique_ctpy:
    ### TOTAL ###
    total_ctpy_df = usd_df[usd_df['First Counterparty Name'] == ctpy]
    long_total_subset = total_ctpy_df[total_ctpy_df['$ NMV'] > 0]
    short_total_subset = total_ctpy_df[total_ctpy_df['$ NMV'] < 0]
    if(len(total_ctpy_df) != 0):
        usd_total_balances_df.loc['Dealer Bids',ctpy] = round(short_total_subset['$ NMV'].sum() / 1000000) * -1
        usd_total_balances_df.loc['Dealer Offers',ctpy] = round(long_total_subset['$ NMV'].sum() / 1000000) * -1
        usd_total_balances_df.loc['Gross',ctpy] =  round((usd_total_balances_df.loc['Dealer Bids',ctpy] * -1) + usd_total_balances_df.loc['Dealer Offers',ctpy]) * -1
        usd_total_balances_df.loc['Net',ctpy] =  round(usd_total_balances_df.loc['Dealer Bids',ctpy] + usd_total_balances_df.loc['Dealer Offers',ctpy])
        usd_total_balances_df.loc['Gross %',ctpy] =  str(round(usd_total_balances_df.loc['Gross',ctpy] / usd_total_balances_df.loc['Gross','Total'] * 100)) + '%'
        usd_total_balances_df.loc['Net %',ctpy] =  str(round(usd_total_balances_df.loc['Net',ctpy] / usd_total_balances_df.loc['Net','Total'] * 100)) + '%'
    
    ### OVERNIGHT ###
    overnight_ctpy_df = usd_overnight_df[usd_overnight_df['First Counterparty Name'] == ctpy]
    long_overnight_subset = overnight_ctpy_df[overnight_ctpy_df['$ NMV'] > 0]
    short_overnight_subset = overnight_ctpy_df[overnight_ctpy_df['$ NMV'] < 0]
    if(len(overnight_ctpy_df) != 0):
        usd_overnight_balances_df.loc['Dealer Bids',ctpy] = round(short_overnight_subset['$ NMV'].sum() / 1000000) * -1
        usd_overnight_balances_df.loc['Dealer Offers',ctpy] = round(long_overnight_subset['$ NMV'].sum() / 1000000) * -1
        usd_overnight_balances_df.loc['Gross',ctpy] =  round((usd_overnight_balances_df.loc['Dealer Bids',ctpy] * -1) + usd_overnight_balances_df.loc['Dealer Offers',ctpy]) * -1
        usd_overnight_balances_df.loc['Net',ctpy] =  round(usd_overnight_balances_df.loc['Dealer Bids',ctpy] + usd_overnight_balances_df.loc['Dealer Offers',ctpy])
        usd_overnight_balances_df.loc['Gross %',ctpy] =  str(round(usd_overnight_balances_df.loc['Gross',ctpy] / usd_overnight_balances_df.loc['Gross','Total'] * 100)) + '%'
        usd_overnight_balances_df.loc['Net %',ctpy] =  str(round(usd_overnight_balances_df.loc['Net',ctpy] / usd_overnight_balances_df.loc['Net','Total'] * 100)) + '%'
    
    ### TERM ###
    term_ctpy_df = usd_term_df[usd_term_df['First Counterparty Name'] == ctpy]
    long_term_subset = term_ctpy_df[term_ctpy_df['$ NMV'] > 0]
    short_term_subset = term_ctpy_df[term_ctpy_df['$ NMV'] < 0]
    if(len(term_ctpy_df) != 0):
        usd_term_balances_df.loc['Dealer Bids',ctpy] = round(short_term_subset['$ NMV'].sum() / 1000000) * -1
        usd_term_balances_df.loc['Dealer Offers',ctpy] = round(long_term_subset['$ NMV'].sum() / 1000000) * -1
        usd_term_balances_df.loc['Gross',ctpy] =  round((usd_term_balances_df.loc['Dealer Bids',ctpy] * -1) + usd_term_balances_df.loc['Dealer Offers',ctpy]) * -1
        usd_term_balances_df.loc['Net',ctpy] =  round(usd_term_balances_df.loc['Dealer Bids',ctpy] + usd_term_balances_df.loc['Dealer Offers',ctpy])
        usd_term_balances_df.loc['Gross %',ctpy] =  str(round(usd_term_balances_df.loc['Gross',ctpy] / usd_term_balances_df.loc['Gross','Total'] * 100)) + '%'
        usd_term_balances_df.loc['Net %',ctpy] =  str(round(usd_term_balances_df.loc['Net',ctpy] / usd_term_balances_df.loc['Net','Total'] * 100)) + '%'
        
    ### HAIRCUT ###
    usd_haircut_df.loc['Gross Balance',ctpy] = round(usd_total_balances_df.loc['Gross',ctpy])
    usd_haircut_df.loc['Net Balance',ctpy] = round(usd_total_balances_df.loc['Net',ctpy])
    usd_haircut_df.loc['Calculated Haircut Amount',ctpy] = round(total_ctpy_df['Haircut Amount'].sum()*-1)
    usd_haircut_df.loc['Realized Haircut % of Gross',ctpy] = str(round(100 * usd_haircut_df.loc['Calculated Haircut Amount',ctpy] / usd_haircut_df.loc['Gross Balance',ctpy],2)) + '%'

### ORGANIZE DATA FRAME ###
usd_total_balances_df = usd_total_balances_df.fillna(0)
usd_total_balances_df.insert(0,'',usd_total_balances_df.index)
usd_overnight_balances_df = usd_overnight_balances_df.fillna(0)
usd_overnight_balances_df.insert(0,'',usd_overnight_balances_df.index)
usd_term_balances_df = usd_term_balances_df.fillna(0)
usd_term_balances_df.insert(0,'',usd_term_balances_df.index)
usd_haircut_df = usd_haircut_df.fillna(0)
usd_haircut_df.insert(0,'',usd_haircut_df.index)

### ------------------------------------------------------------------ TJM JPN REPO ANALYSIS ------------------------------------------------------------------ ###

### CREATE DATAFRAMES ###
today_on_tca_bps_tjm = pd.DataFrame(columns = colnames,
                                    index = ['All','Total','Dealer Bids','Dealer Offers',
                                             'Specials','Total','Dealer Bids','Dealer Offers',
                                             'General Collateral','Total','Dealer Bids','Dealer Offers'])
ytd_on_tca_bps_tjm = pd.DataFrame(columns = colnames,
                                  index = ['All','Total','Dealer Bids','Dealer Offers',
                                           'Specials','Total','Dealer Bids','Dealer Offers',
                                           'General Collateral','Total','Dealer Bids','Dealer Offers'])
today_on_tca_thou_tjm = pd.DataFrame(columns = colnames,
                                     index = ['All','Total','Dealer Bids','Dealer Offers',
                                              'Specials','Total','Dealer Bids','Dealer Offers',
                                              'General Collateral','Total','Dealer Bids','Dealer Offers'])
ytd_on_tca_thou_tjm = pd.DataFrame(columns = colnames,
                                   index = ['All','Total','Dealer Bids','Dealer Offers',
                                            'Specials','Total','Dealer Bids','Dealer Offers',
                                            'General Collateral','Total','Dealer Bids','Dealer Offers'])

tjm_gc_rate = tjm_df[tjm_df['name'] == 'General Collateral']
jpm_gc_rate = float(jpm_df[jpm_df['name'] == 'General Collateral']['avg'])

jpm_df

### -------------------------------------------------------------------------- EMAIL -------------------------------------------------------------------------- ###

### EXPORT AS EXCEL FILE FIRST ###
excel_export_string = (r"C:\Users\ChenChen\OC Investment Management, LP\Trading - General\Chen\Repo Balance" 
                       + "\Repo Balance " + today_date_string + ".xlsx")
usd_total_balances_df.to_excel(excel_export_string)

### CUSTOM FUNCTIONS ###
def create_email(input_df1:pd.DataFrame,
                 input_df2:pd.DataFrame,
                 input_df3:pd.DataFrame,
                 input_df4:pd.DataFrame,
                 input_df5:pd.DataFrame,
                 input_df6:pd.DataFrame,
                 input_df7:pd.DataFrame,
                 input_df8:pd.DataFrame) -> str:
    data1 = build_table(usd_total_balances_df,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data2 = build_table(input_df2,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data3 = build_table(input_df3,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data4 = build_table(input_df4,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data5 = build_table(input_df5,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data6 = build_table(input_df6,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data7 = build_table(input_df7,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    data8 = build_table(input_df8,
                        'blue_light',
                        font_size='15px',
                        text_align='center',
                        width = '100px')
    soup1 = BeautifulSoup(data1, 'html.parser')
    soup2 = BeautifulSoup(data2, 'html.parser')
    soup3 = BeautifulSoup(data3, 'html.parser')
    soup4 = BeautifulSoup(data4, 'html.parser')
    soup5 = BeautifulSoup(data5, 'html.parser')
    soup6 = BeautifulSoup(data6, 'html.parser')
    soup7 = BeautifulSoup(data7, 'html.parser')
    soup8 = BeautifulSoup(data8, 'html.parser')
    
    for x in soup1.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup2.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup3.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup4.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup5.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup6.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup7.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    for x in soup8.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                final_val = f'({abs(int_data):,})'
                x['style'] = x['style'].replace('color: black', 'color: red')
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    
    return ('<b> Table 1: USD Total Balances' + str(soup1) + 
            'Table 2: USD Overnight Balances' + str(soup2) + 
            'Table 3: USD Term Balances' + str(soup3) + 
            'Table 4: USD Today Haircut Analysis' + str(soup4) +
            'Table 5: Today Overnight TCA: Summary Table (bps from TJM avg)' + str(soup5) +
            'Table 6: YTD Overnight TCA: Summary Table (bps from TJM avg)' + str(soup6) +
            'Table 7: Today Overnight TCA: Summary Table (dollar cost vs. TJM avg in thousands)' + str(soup7) +
            'Table 8: YTD Overnight TCA: Summary Table (dollar cost vs. TJM avg in thousands)' + str(soup8))

### EMAIL ###
exec(open(r'C:\Users\ChenChen\OneDrive - OC Investment Management, LP\Documents\GitHub\Chen\Enfusion-Ops\EmailClient.py').read())
ABS_PATH = os.path.dirname(os.path.abspath(excel_export_string)) # get path of file
email = EmailClient()
email_title = 'USD Repo Balance Summary: ' + today_dt
email.send_test_email(email_title,create_email(usd_total_balances_df,
                                               usd_overnight_balances_df,
                                               usd_term_balances_df,
                                               usd_haircut_df))