### ------------------------------------------------------------------- REPO BALANCE SUMMARY ------------------------------------------------------------------- ###

### PACKAGES ###
import pandas as pd
from datetime import date
import os         
import numpy as np                                                    
from pretty_html_table import build_table
from bs4 import BeautifulSoup

### CLIENTS ###
import scripts.clients.new_enfusion_client as enf

### ------------------------------------------------------------------------ DATA PULL ------------------------------------------------------------------------ ###

### ENFUSION ###
client = enf.EnfusionClient('serviceuser@ocimgt.com', 'Enfusion2023!')
live_repos_positions = client.get_live_repos_today_df()

### ISOLATE COUNTRIES ###
gbp_df = live_repos_positions[live_repos_positions['Risk Country'] == 'United Kingdom']
gbp_df = gbp_df[gbp_df['First Counterparty Name'] != 'Internal']
gbp_df['Haircut Amount'] = ((1 - gbp_df['Repurchase Agreement Haircut']) * gbp_df['$ NMV']) / 1000000
unique_ctpy_array = gbp_df['First Counterparty Name'].unique()
colnames = np.append('Total',unique_ctpy_array)

### DATES ###
today_date = date.today()
today_date_string = today_date.strftime("%m%d%Y")
today_dt = today_date.strftime("%m/%d/%Y")

### ------------------------------------------------------------------ REPO BALANCE ANALYSIS ------------------------------------------------------------------ ###

### CREATE DATAFRAME FOR ALL COUNTERPARTIES AND FINAL EXPORT ###
gbp_total_balances_df = pd.DataFrame(columns = colnames,
                                     index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
gbp_overnight_balances_df = pd.DataFrame(columns = colnames,
                                                    index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
gbp_term_balances_df = pd.DataFrame(columns = colnames,
                                    index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
gbp_haircut_df = pd.DataFrame(columns = colnames,
                              index = ['Gross Balance','Net Balance','Calculated Haircut Amount','Realized Haircut % of Gross'])

### TOTAL COLUMNS - TOTAL BALANCES###
gbp_total_balances_df.loc['Dealer Bids','Total'] = round(gbp_df[gbp_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
gbp_total_balances_df.loc['Dealer Offers','Total'] = round(gbp_df[gbp_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
gbp_total_balances_df.loc['Gross','Total'] = round((gbp_total_balances_df.loc['Dealer Bids','Total'] * -1) + gbp_total_balances_df.loc['Dealer Offers','Total']) * -1
gbp_total_balances_df.loc['Net','Total'] = round(gbp_total_balances_df.loc['Dealer Bids','Total'] + gbp_total_balances_df.loc['Dealer Offers','Total'])
gbp_total_balances_df.loc['Gross %','Total'] =  str(round(gbp_total_balances_df.loc['Gross','Total'] / gbp_total_balances_df.loc['Gross','Total'] * 100)) + '%'
gbp_total_balances_df.loc['Net %','Total'] =  str(round(gbp_total_balances_df.loc['Net','Total'] / gbp_total_balances_df.loc['Net','Total'] * 100)) + '%'

### HAIRCUT ANALYSIS ###
gbp_haircut_df.loc['Gross Balance','Total'] = round(((gbp_total_balances_df.loc['Dealer Bids','Total'] * -1) + gbp_total_balances_df.loc['Dealer Offers','Total']) * -1)
gbp_haircut_df.loc['Net Balance','Total'] = round(gbp_total_balances_df.loc['Dealer Bids','Total'] + gbp_total_balances_df.loc['Dealer Offers','Total'])
gbp_haircut_df.loc['Calculated Haircut Amount','Total'] = round(gbp_df['Haircut Amount'].sum() * -1)
gbp_haircut_df.loc['Realized Haircut % of Gross','Total'] = str(round(100 * gbp_haircut_df.loc['Calculated Haircut Amount','Total'] / gbp_haircut_df.loc['Gross Balance','Total'],2)) + '%'

### TOTAL COLUMNS - TOTAL BALANCES###
gbp_overnight_df = gbp_df[gbp_df['Days To Maturity'] == 1]
if(len(gbp_overnight_df) != 0):
    gbp_overnight_balances_df.loc['Dealer Bids','Total'] = round(gbp_overnight_df[gbp_overnight_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
    gbp_overnight_balances_df.loc['Dealer Offers','Total'] = round(gbp_overnight_df[gbp_overnight_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
    gbp_overnight_balances_df.loc['Gross','Total'] =  round((gbp_overnight_balances_df.loc['Dealer Bids','Total'] * -1) + gbp_overnight_balances_df.loc['Dealer Offers','Total']) * -1
    gbp_overnight_balances_df.loc['Net','Total'] =  round(gbp_overnight_balances_df.loc['Dealer Bids','Total'] + gbp_overnight_balances_df.loc['Dealer Offers','Total'])
    gbp_overnight_balances_df.loc['Gross %','Total'] =  str(round(gbp_overnight_balances_df.loc['Gross','Total'] / gbp_overnight_balances_df.loc['Gross','Total'] * 100)) + '%'
    gbp_overnight_balances_df.loc['Net %','Total'] =  str(round(gbp_overnight_balances_df.loc['Net','Total'] / gbp_overnight_balances_df.loc['Net','Total'] * 100)) + '%'

### TOTAL COLUMNS - TOTAL BALANCES###
gbp_term_df = gbp_df[gbp_df['Days To Maturity'] > 1]
if(len(gbp_term_df) != 0):
    gbp_term_balances_df.loc['Dealer Bids','Total'] = round(gbp_term_df[gbp_term_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
    gbp_term_balances_df.loc['Dealer Offers','Total'] = round(gbp_term_df[gbp_term_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
    gbp_term_balances_df.loc['Gross','Total'] =  round((gbp_term_balances_df.loc['Dealer Bids','Total'] * -1) + gbp_term_balances_df.loc['Dealer Offers','Total']) * -1
    gbp_term_balances_df.loc['Net','Total'] =  round(gbp_term_balances_df.loc['Dealer Bids','Total'] + gbp_term_balances_df.loc['Dealer Offers','Total'])
    gbp_term_balances_df.loc['Gross %','Total'] =  str(round(gbp_term_balances_df.loc['Gross','Total'] / gbp_term_balances_df.loc['Gross','Total'] * 100)) + '%'
    gbp_term_balances_df.loc['Net %','Total'] =  str(round(gbp_term_balances_df.loc['Net','Total'] / gbp_term_balances_df.loc['Net','Total'] * 100)) + '%'

### FOR EACH COUNTERPARTY ###
unique_ctpy = gbp_df['First Counterparty Name'].unique()
for ctpy in unique_ctpy:
    ### TOTAL ###
    total_ctpy_df = gbp_df[gbp_df['First Counterparty Name'] == ctpy]
    long_total_subset = total_ctpy_df[total_ctpy_df['$ NMV'] > 0]
    short_total_subset = total_ctpy_df[total_ctpy_df['$ NMV'] < 0]
    if(len(total_ctpy_df) != 0):
        gbp_total_balances_df.loc['Dealer Bids',ctpy] = round(short_total_subset['$ NMV'].sum() / 1000000) * -1
        gbp_total_balances_df.loc['Dealer Offers',ctpy] = round(long_total_subset['$ NMV'].sum() / 1000000) * -1
        gbp_total_balances_df.loc['Gross',ctpy] =  round((gbp_total_balances_df.loc['Dealer Bids',ctpy] * -1) + gbp_total_balances_df.loc['Dealer Offers',ctpy]) * -1
        gbp_total_balances_df.loc['Net',ctpy] =  round(gbp_total_balances_df.loc['Dealer Bids',ctpy] + gbp_total_balances_df.loc['Dealer Offers',ctpy])
        gbp_total_balances_df.loc['Gross %',ctpy] =  str(round(gbp_total_balances_df.loc['Gross',ctpy] / gbp_total_balances_df.loc['Gross','Total'] * 100)) + '%'
        gbp_total_balances_df.loc['Net %',ctpy] =  str(round(gbp_total_balances_df.loc['Net',ctpy] / gbp_total_balances_df.loc['Net','Total'] * 100)) + '%'
    
    ### OVERNIGHT ###
    overnight_ctpy_df = gbp_overnight_df[gbp_overnight_df['First Counterparty Name'] == ctpy]
    long_overnight_subset = overnight_ctpy_df[overnight_ctpy_df['$ NMV'] > 0]
    short_overnight_subset = overnight_ctpy_df[overnight_ctpy_df['$ NMV'] < 0]
    if(len(overnight_ctpy_df) != 0):
        gbp_overnight_balances_df.loc['Dealer Bids',ctpy] = round(short_overnight_subset['$ NMV'].sum() / 1000000) * -1
        gbp_overnight_balances_df.loc['Dealer Offers',ctpy] = round(long_overnight_subset['$ NMV'].sum() / 1000000) * -1
        gbp_overnight_balances_df.loc['Gross',ctpy] =  round((gbp_overnight_balances_df.loc['Dealer Bids',ctpy] * -1) + gbp_overnight_balances_df.loc['Dealer Offers',ctpy]) * -1
        gbp_overnight_balances_df.loc['Net',ctpy] =  round(gbp_overnight_balances_df.loc['Dealer Bids',ctpy] + gbp_overnight_balances_df.loc['Dealer Offers',ctpy])
        gbp_overnight_balances_df.loc['Gross %',ctpy] =  str(round(gbp_overnight_balances_df.loc['Gross',ctpy] / gbp_overnight_balances_df.loc['Gross','Total'] * 100)) + '%'
        gbp_overnight_balances_df.loc['Net %',ctpy] =  str(round(gbp_overnight_balances_df.loc['Net',ctpy] / gbp_overnight_balances_df.loc['Net','Total'] * 100)) + '%'
    
    ### TERM ###
    term_ctpy_df = gbp_term_df[gbp_term_df['First Counterparty Name'] == ctpy]
    long_term_subset = term_ctpy_df[term_ctpy_df['$ NMV'] > 0]
    short_term_subset = term_ctpy_df[term_ctpy_df['$ NMV'] < 0]
    if(len(term_ctpy_df) != 0):
        gbp_term_balances_df.loc['Dealer Bids',ctpy] = round(short_term_subset['$ NMV'].sum() / 1000000) * -1
        gbp_term_balances_df.loc['Dealer Offers',ctpy] = round(long_term_subset['$ NMV'].sum() / 1000000) * -1
        gbp_term_balances_df.loc['Gross',ctpy] =  round((gbp_term_balances_df.loc['Dealer Bids',ctpy] * -1) + gbp_term_balances_df.loc['Dealer Offers',ctpy]) * -1
        gbp_term_balances_df.loc['Net',ctpy] =  round(gbp_term_balances_df.loc['Dealer Bids',ctpy] + gbp_term_balances_df.loc['Dealer Offers',ctpy])
        gbp_term_balances_df.loc['Gross %',ctpy] =  str(round(gbp_term_balances_df.loc['Gross',ctpy] / gbp_term_balances_df.loc['Gross','Total'] * 100)) + '%'
        gbp_term_balances_df.loc['Net %',ctpy] =  str(round(gbp_term_balances_df.loc['Net',ctpy] / gbp_term_balances_df.loc['Net','Total'] * 100)) + '%'
        
    ### HAIRCUT ###
    gbp_haircut_df.loc['Gross Balance',ctpy] = round(gbp_total_balances_df.loc['Gross',ctpy])
    gbp_haircut_df.loc['Net Balance',ctpy] = round(gbp_total_balances_df.loc['Net',ctpy])
    gbp_haircut_df.loc['Calculated Haircut Amount',ctpy] = round(total_ctpy_df['Haircut Amount'].sum()*-1)
    gbp_haircut_df.loc['Realized Haircut % of Gross',ctpy] = str(round(100 * gbp_haircut_df.loc['Calculated Haircut Amount',ctpy] / gbp_haircut_df.loc['Gross Balance',ctpy],2)) + '%'

### ORGANIZE DATA FRAME ###
gbp_total_balances_df = gbp_total_balances_df.fillna(0)
gbp_total_balances_df.insert(0,'',gbp_total_balances_df.index)
gbp_overnight_balances_df = gbp_overnight_balances_df.fillna(0)
gbp_overnight_balances_df.insert(0,'',gbp_overnight_balances_df.index)
gbp_term_balances_df = gbp_term_balances_df.fillna(0)
gbp_term_balances_df.insert(0,'',gbp_term_balances_df.index)
gbp_haircut_df = gbp_haircut_df.fillna(0)
gbp_haircut_df.insert(0,'',gbp_haircut_df.index)

### -------------------------------------------------------------------------- EMAIL -------------------------------------------------------------------------- ###

### EXPORT AS EXCEL FILE FIRST ###
excel_export_string = (r"C:\Users\ChenChen\OC Investment Management, LP\Trading - General\Chen\Repo Balance" 
                       + "\Repo Balance " + today_date_string + ".xlsx")
gbp_total_balances_df.to_excel(excel_export_string)

### CUSTOM FUNCTIONS ###
def create_email(input_df1:pd.DataFrame,
                 input_df2:pd.DataFrame,
                 input_df3:pd.DataFrame,
                 input_df4:pd.DataFrame) -> str:
    data1 = build_table(input_df1,
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
    soup1 = BeautifulSoup(data1, 'html.parser')
    soup2 = BeautifulSoup(data2, 'html.parser')
    soup3 = BeautifulSoup(data3, 'html.parser')
    soup4 = BeautifulSoup(data4, 'html.parser')
    
    for x in soup1.findAll('td'):
        try:
            int_data = int(x.string)
            if int_data < 0:
                if('; color: ' in x['style']):
                    x['style'] = x['style'].replace('color: black', 'color: red')
                else:
                    font_family_locator = x['style'].find('font-family')
                    x['style'] = x['style'][:font_family_locator] + ' color: red;' + x['style'][font_family_locator:]
                final_val = f'({abs(int_data):,})'
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
                if('; color: ' in x['style']):
                    x['style'] = x['style'].replace('color: black', 'color: red')
                else:
                    font_family_locator = x['style'].find('font-family')
                    x['style'] = x['style'][:font_family_locator] + ' color: red;' + x['style'][font_family_locator:]
                final_val = f'({abs(int_data):,})'
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
                if('; color: ' in x['style']):
                    x['style'] = x['style'].replace('color: black', 'color: red')
                else:
                    font_family_locator = x['style'].find('font-family')
                    x['style'] = x['style'][:font_family_locator] + ' color: red;' + x['style'][font_family_locator:]
                final_val = f'({abs(int_data):,})'
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
                if('; color: ' in x['style']):
                    x['style'] = x['style'].replace('color: black', 'color: red')
                else:
                    font_family_locator = x['style'].find('font-family')
                    x['style'] = x['style'][:font_family_locator] + ' color: red;' + x['style'][font_family_locator:]
                final_val = f'({abs(int_data):,})'
            else:
                final_val = f'{int_data:,}'
            x.string.replace_with(final_val)
        except:
            if('0' not in x.string and '1' not in x.string and '2' not in x.string and '3' not in x.string and '4' not in x.string and 
               '5' not in x.string and '6' not in x.string and '7' not in x.string and '8' not in x.string and '9' not in x.string):
                x['style'] = x['style'].replace('text-align: center', 'text-align: left')
    
    return ('<b> Table 1: GBP Total Balances' + str(soup1) + 
            'Table 2: GBP Overnight Balances' + str(soup2) + 
            'Table 3: GBP Term Balances' + str(soup3) + 
            'Table 4: GBP Today Haircut Analysis' + str(soup4))

### EMAIL ###
exec(open(r'C:\Users\ChenChen\OneDrive - OC Investment Management, LP\Documents\GitHub\Chen\Enfusion-Ops\EmailClient.py').read())
ABS_PATH = os.path.dirname(os.path.abspath(excel_export_string)) # get path of file
email = EmailClient()
email_title = 'GBP Repo Balance Summary: ' + today_dt
email.send_test_email(email_title,create_email(gbp_total_balances_df,
                                               gbp_overnight_balances_df,
                                               gbp_term_balances_df,
                                               gbp_haircut_df))