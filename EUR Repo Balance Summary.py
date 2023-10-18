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
france = live_repos_positions[live_repos_positions['Risk Country'] == 'France']
germany = live_repos_positions[live_repos_positions['Risk Country'] == 'Germany']
netherlands = live_repos_positions[live_repos_positions['Risk Country'] == 'Netherlands']
eur_df= pd.concat([france,germany,netherlands])
eur_df = eur_df[eur_df['First Counterparty Name'] != 'Internal']
eur_df['Haircut Amount'] = ((1 - eur_df['Repurchase Agreement Haircut']) * eur_df['$ NMV']) / 1000000
unique_ctpy_array = eur_df['First Counterparty Name'].unique()
colnames = np.append('Total',unique_ctpy_array)

### DATES ###
today_date = date.today()
today_date_string = today_date.strftime("%m%d%Y")
today_dt = today_date.strftime("%m/%d/%Y")

### ------------------------------------------------------------------ REPO BALANCE ANALYSIS ------------------------------------------------------------------ ###

### CREATE DATAFRAME FOR ALL COUNTERPARTIES AND FINAL EXPORT ###
eur_total_balances_df = pd.DataFrame(columns = colnames,
                                     index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
eur_overnight_balances_df = pd.DataFrame(columns = colnames,
                                                    index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
eur_term_balances_df = pd.DataFrame(columns = colnames,
                                    index = ['Dealer Bids','Dealer Offers','Gross','Net','Gross %','Net %'])
eur_haircut_df = pd.DataFrame(columns = colnames,
                              index = ['Gross Balance','Net Balance','Calculated Haircut Amount','Realized Haircut % of Gross'])

### TOTAL COLUMNS - TOTAL BALANCES###
eur_total_balances_df.loc['Dealer Bids','Total'] = round(eur_df[eur_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
eur_total_balances_df.loc['Dealer Offers','Total'] = round(eur_df[eur_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
eur_total_balances_df.loc['Gross','Total'] = round((eur_total_balances_df.loc['Dealer Bids','Total'] * -1) + eur_total_balances_df.loc['Dealer Offers','Total']) * -1
eur_total_balances_df.loc['Net','Total'] = round(eur_total_balances_df.loc['Dealer Bids','Total'] + eur_total_balances_df.loc['Dealer Offers','Total'])
eur_total_balances_df.loc['Gross %','Total'] =  str(round(eur_total_balances_df.loc['Gross','Total'] / eur_total_balances_df.loc['Gross','Total'] * 100)) + '%'
eur_total_balances_df.loc['Net %','Total'] =  str(round(eur_total_balances_df.loc['Net','Total'] / eur_total_balances_df.loc['Net','Total'] * 100)) + '%'

### HAIRCUT ANALYSIS ###
eur_haircut_df.loc['Gross Balance','Total'] = round(((eur_total_balances_df.loc['Dealer Bids','Total'] * -1) + eur_total_balances_df.loc['Dealer Offers','Total']) * -1)
eur_haircut_df.loc['Net Balance','Total'] = round(eur_total_balances_df.loc['Dealer Bids','Total'] + eur_total_balances_df.loc['Dealer Offers','Total'])
eur_haircut_df.loc['Calculated Haircut Amount','Total'] = round(eur_df['Haircut Amount'].sum() * -1)
eur_haircut_df.loc['Realized Haircut % of Gross','Total'] = str(round(100 * eur_haircut_df.loc['Calculated Haircut Amount','Total'] / eur_haircut_df.loc['Gross Balance','Total'],2)) + '%'

### TOTAL COLUMNS - TOTAL BALANCES###
eur_overnight_df = eur_df[eur_df['Days To Maturity'] == 1]
if(len(eur_overnight_df) != 0):
    eur_overnight_balances_df.loc['Dealer Bids','Total'] = round(eur_overnight_df[eur_overnight_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
    eur_overnight_balances_df.loc['Dealer Offers','Total'] = round(eur_overnight_df[eur_overnight_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
    eur_overnight_balances_df.loc['Gross','Total'] =  round((eur_overnight_balances_df.loc['Dealer Bids','Total'] * -1) + eur_overnight_balances_df.loc['Dealer Offers','Total']) * -1
    eur_overnight_balances_df.loc['Net','Total'] =  round(eur_overnight_balances_df.loc['Dealer Bids','Total'] + eur_overnight_balances_df.loc['Dealer Offers','Total'])
    eur_overnight_balances_df.loc['Gross %','Total'] =  str(round(eur_overnight_balances_df.loc['Gross','Total'] / eur_overnight_balances_df.loc['Gross','Total'] * 100)) + '%'
    eur_overnight_balances_df.loc['Net %','Total'] =  str(round(eur_overnight_balances_df.loc['Net','Total'] / eur_overnight_balances_df.loc['Net','Total'] * 100)) + '%'

### TOTAL COLUMNS - TOTAL BALANCES###
eur_term_df = eur_df[eur_df['Days To Maturity'] > 1]
if(len(eur_term_df) != 0):
    eur_term_balances_df.loc['Dealer Bids','Total'] = round(eur_term_df[eur_term_df['$ NMV'] < 0]['$ NMV'].sum() / 1000000) * -1
    eur_term_balances_df.loc['Dealer Offers','Total'] = round(eur_term_df[eur_term_df['$ NMV'] > 0]['$ NMV'].sum() / 1000000) * -1
    eur_term_balances_df.loc['Gross','Total'] =  round((eur_term_balances_df.loc['Dealer Bids','Total'] * -1) + eur_term_balances_df.loc['Dealer Offers','Total']) * -1
    eur_term_balances_df.loc['Net','Total'] =  round(eur_term_balances_df.loc['Dealer Bids','Total'] + eur_term_balances_df.loc['Dealer Offers','Total'])
    eur_term_balances_df.loc['Gross %','Total'] =  str(round(eur_term_balances_df.loc['Gross','Total'] / eur_term_balances_df.loc['Gross','Total'] * 100)) + '%'
    eur_term_balances_df.loc['Net %','Total'] =  str(round(eur_term_balances_df.loc['Net','Total'] / eur_term_balances_df.loc['Net','Total'] * 100)) + '%'

### FOR EACH COUNTERPARTY ###
unique_ctpy = eur_df['First Counterparty Name'].unique()
for ctpy in unique_ctpy:
    ### TOTAL ###
    total_ctpy_df = eur_df[eur_df['First Counterparty Name'] == ctpy]
    long_total_subset = total_ctpy_df[total_ctpy_df['$ NMV'] > 0]
    short_total_subset = total_ctpy_df[total_ctpy_df['$ NMV'] < 0]
    if(len(total_ctpy_df) != 0):
        eur_total_balances_df.loc['Dealer Bids',ctpy] = round(short_total_subset['$ NMV'].sum() / 1000000) * -1
        eur_total_balances_df.loc['Dealer Offers',ctpy] = round(long_total_subset['$ NMV'].sum() / 1000000) * -1
        eur_total_balances_df.loc['Gross',ctpy] =  round((eur_total_balances_df.loc['Dealer Bids',ctpy] * -1) + eur_total_balances_df.loc['Dealer Offers',ctpy]) * -1
        eur_total_balances_df.loc['Net',ctpy] =  round(eur_total_balances_df.loc['Dealer Bids',ctpy] + eur_total_balances_df.loc['Dealer Offers',ctpy])
        eur_total_balances_df.loc['Gross %',ctpy] =  str(round(eur_total_balances_df.loc['Gross',ctpy] / eur_total_balances_df.loc['Gross','Total'] * 100)) + '%'
        eur_total_balances_df.loc['Net %',ctpy] =  str(round(eur_total_balances_df.loc['Net',ctpy] / eur_total_balances_df.loc['Net','Total'] * 100)) + '%'
    
    ### OVERNIGHT ###
    overnight_ctpy_df = eur_overnight_df[eur_overnight_df['First Counterparty Name'] == ctpy]
    long_overnight_subset = overnight_ctpy_df[overnight_ctpy_df['$ NMV'] > 0]
    short_overnight_subset = overnight_ctpy_df[overnight_ctpy_df['$ NMV'] < 0]
    if(len(overnight_ctpy_df) != 0):
        eur_overnight_balances_df.loc['Dealer Bids',ctpy] = round(short_overnight_subset['$ NMV'].sum() / 1000000) * -1
        eur_overnight_balances_df.loc['Dealer Offers',ctpy] = round(long_overnight_subset['$ NMV'].sum() / 1000000) * -1
        eur_overnight_balances_df.loc['Gross',ctpy] =  round((eur_overnight_balances_df.loc['Dealer Bids',ctpy] * -1) + eur_overnight_balances_df.loc['Dealer Offers',ctpy]) * -1
        eur_overnight_balances_df.loc['Net',ctpy] =  round(eur_overnight_balances_df.loc['Dealer Bids',ctpy] + eur_overnight_balances_df.loc['Dealer Offers',ctpy])
        eur_overnight_balances_df.loc['Gross %',ctpy] =  str(round(eur_overnight_balances_df.loc['Gross',ctpy] / eur_overnight_balances_df.loc['Gross','Total'] * 100)) + '%'
        eur_overnight_balances_df.loc['Net %',ctpy] =  str(round(eur_overnight_balances_df.loc['Net',ctpy] / eur_overnight_balances_df.loc['Net','Total'] * 100)) + '%'
    
    ### TERM ###
    term_ctpy_df = eur_term_df[eur_term_df['First Counterparty Name'] == ctpy]
    long_term_subset = term_ctpy_df[term_ctpy_df['$ NMV'] > 0]
    short_term_subset = term_ctpy_df[term_ctpy_df['$ NMV'] < 0]
    if(len(term_ctpy_df) != 0):
        eur_term_balances_df.loc['Dealer Bids',ctpy] = round(short_term_subset['$ NMV'].sum() / 1000000) * -1
        eur_term_balances_df.loc['Dealer Offers',ctpy] = round(long_term_subset['$ NMV'].sum() / 1000000) * -1
        eur_term_balances_df.loc['Gross',ctpy] =  round((eur_term_balances_df.loc['Dealer Bids',ctpy] * -1) + eur_term_balances_df.loc['Dealer Offers',ctpy]) * -1
        eur_term_balances_df.loc['Net',ctpy] =  round(eur_term_balances_df.loc['Dealer Bids',ctpy] + eur_term_balances_df.loc['Dealer Offers',ctpy])
        eur_term_balances_df.loc['Gross %',ctpy] =  str(round(eur_term_balances_df.loc['Gross',ctpy] / eur_term_balances_df.loc['Gross','Total'] * 100)) + '%'
        eur_term_balances_df.loc['Net %',ctpy] =  str(round(eur_term_balances_df.loc['Net',ctpy] / eur_term_balances_df.loc['Net','Total'] * 100)) + '%'
        
    ### HAIRCUT ###
    eur_haircut_df.loc['Gross Balance',ctpy] = round(eur_total_balances_df.loc['Gross',ctpy])
    eur_haircut_df.loc['Net Balance',ctpy] = round(eur_total_balances_df.loc['Net',ctpy])
    eur_haircut_df.loc['Calculated Haircut Amount',ctpy] = round(total_ctpy_df['Haircut Amount'].sum()*-1)
    eur_haircut_df.loc['Realized Haircut % of Gross',ctpy] = str(round(100 * eur_haircut_df.loc['Calculated Haircut Amount',ctpy] / eur_haircut_df.loc['Gross Balance',ctpy],2)) + '%'

### ORGANIZE DATA FRAME ###
eur_total_balances_df = eur_total_balances_df.fillna(0)
eur_total_balances_df.insert(0,'',eur_total_balances_df.index)
eur_overnight_balances_df = eur_overnight_balances_df.fillna(0)
eur_overnight_balances_df.insert(0,'',eur_overnight_balances_df.index)
eur_term_balances_df = eur_term_balances_df.fillna(0)
eur_term_balances_df.insert(0,'',eur_term_balances_df.index)
eur_haircut_df = eur_haircut_df.fillna(0)
eur_haircut_df.insert(0,'',eur_haircut_df.index)

### -------------------------------------------------------------------------- EMAIL -------------------------------------------------------------------------- ###

### EXPORT AS EXCEL FILE FIRST ###
excel_export_string = (r"C:\Users\ChenChen\OC Investment Management, LP\Trading - General\Chen\Repo Balance" 
                       + "\Repo Balance " + today_date_string + ".xlsx")
eur_total_balances_df.to_excel(excel_export_string)

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
                final_val = f'({abs(int_data):,})'
                if('; color: ' in x['style']):
                    x['style'] = x['style'].replace('color: black', 'color: red')
                else:
                    font_family_locator = x['style'].find('font-family')
                    x['style'] = x['style'][:font_family_locator] + ' color: red;' + x['style'][font_family_locator:]
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
    
    return ('<b> Table 1: EUR Total Balances' + str(soup1) + 
            'Table 2: EUR Overnight Balances' + str(soup2) + 
            'Table 3: EUR Term Balances' + str(soup3) + 
            'Table 4: EUR Today Haircut Analysis' + str(soup4))

### EMAIL ###
exec(open(r'C:\Users\ChenChen\OneDrive - OC Investment Management, LP\Documents\GitHub\Chen\Enfusion-Ops\EmailClient.py').read())
ABS_PATH = os.path.dirname(os.path.abspath(excel_export_string)) # get path of file
email = EmailClient()
email_title = 'EUR Repo Balance Summary: ' + today_dt
email.send_test_email(email_title,create_email(eur_total_balances_df,
                                               eur_overnight_balances_df,
                                               eur_term_balances_df,
                                               eur_haircut_df))