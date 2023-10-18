### ----------------------------------------------------------- REPO REPORT ----------------------------------------------------------- ###

### FUNCTIONS AND PACKAGES ###
import pandas as pd
from datetime import date
from pretty_html_table import build_table
import numpy as np
import pandas_market_calendars as mcal

### CLIENTS ###
import scripts.clients.enfusion_api_client as enf

### ------------------------------------------------------------ DATA PULL ------------------------------------------------------------ ###

### PULL ENFUSION ###
client = enf.EnfusionClient('serviceuser@ocimgt.com', 'Enfusion2023!')

### REPO LADDER ###
repo_ladder = client.get_repo_ladder_df()
repo_ladder_df = repo_ladder.tail(-1)
repo_ladder_df = repo_ladder_df.reset_index()
repo_ladder_df = repo_ladder_df.drop('index',axis=1)
repo_ladder_df['Position Scenario Date Adjusted'] = pd.to_datetime(repo_ladder_df['Position Scenario Date Adjusted']).dt.date

### REPO RISK ###
repo_risk = client.get_repo_risk_df()
repo_risk_df = repo_risk.tail(-1)
repo_risk_df = repo_risk_df.reset_index()
repo_risk_df = repo_risk_df.drop('index',axis=1)
repo_risk_df['Current Date'] = pd.to_datetime(repo_risk_df['Current Date']).dt.date

### OPEN ENDED REPO TRADES ###
open_ended_repo_trades = client.get_open_ended_repo_trades_df()
open_ended_repo_trades = open_ended_repo_trades.reset_index()
open_ended_repo_trades = open_ended_repo_trades.drop('index',axis=1)
open_ended_repo_trades['Trade Date'] = pd.to_datetime(open_ended_repo_trades['Trade Date']).dt.date

### ------------------------------------------------------ REPO LADDER DATAFRAME ------------------------------------------------------ ###

### OBTAIN TODAY AND TOMORROW DAYS ###
nyse = mcal.get_calendar('NYSE')
historical_trading_dates = pd.Series(nyse.schedule(start_date = '1949-12-31', end_date = '2039-12-31').index)
trade_dates = np.array([x.to_pydatetime().date() for x in historical_trading_dates])
today_date = date.today()
today_date_index = np.where(trade_dates == today_date)[0][0]
tmrw_date_index = today_date_index + 1
tmrw_date = trade_dates[tmrw_date_index]
next_trade_date_string = tmrw_date.strftime("%m/%d/%Y").replace("/0", "/")
if(next_trade_date_string[0] == '0'):
    next_trade_date_string = next_trade_date_string[1:]

### REPO ANALYSIS OF OVERBORROWING AND ISOLATE ZEROES AND ONLY T+1 OF TOMORROW SETTLEMENT ###
today_subset = repo_ladder_df[repo_ladder_df['Position Scenario Date Adjusted'] == today_date]
repo_today = pd.DataFrame()
repo_today['Repo Start'] = today_subset['Position Scenario Date Adjusted']
repo_today['Description'] = today_subset['Description']
repo_today['Total Amount to Show'] = today_subset['Settled Notional Quantity'] #####################
repo_today = repo_today[repo_today['Total Amount to Show'] != 0]

### ------------------------------------------------------- REPO RISK DATAFRAME ------------------------------------------------------- ###

### SUBSET DATA ###
dum1_subset = repo_risk_df[repo_risk_df['Adjusted Maturity Date'] == next_trade_date_string] #####################
ctpy_subset = dum1_subset[dum1_subset['First Counterparty Name'] == 'Internal']
expected_bond_pos_tmrw_subset = ctpy_subset[ctpy_subset['Book Name'] == 'USD.BAS.TY.HT']

### ------------------------------------------------------- MERGE DATA ANALYSIS ------------------------------------------------------- ###

### ORGANIZE DATA AND MERGE ###
repo_today.index = repo_today['Description']
expected_bond_pos_tmrw_subset.index = expected_bond_pos_tmrw_subset['Underlying Description']
expected_bond_pos_tmrw_subset = expected_bond_pos_tmrw_subset['Notional Quantity']
repo_report_df = pd.merge(expected_bond_pos_tmrw_subset, repo_today, left_index=True, right_index=True, how='outer')
repo_report_df = repo_report_df.fillna(0)
repo_report_df['OCIM Direction'] = np.nan
repo_report_df['Dealer Side'] = np.nan
overborrowed_df = pd.DataFrame()

### FILTER FOR OVERBORROWED US REPO ONLY ###
repo_report_df['Description'] = repo_report_df.index
repo_report_df = repo_report_df.reset_index()
for row in range(0,len(repo_report_df)):
    if('UST' in repo_report_df.at[row,'Description']):
        if(repo_report_df.at[row,'Total Amount to Show'] <= 0):
            repo_report_df.at[row,'OCIM Direction'] = 'Borrow'
            repo_report_df.at[row,'Dealer Side'] = 'Offer'
        else:
            repo_report_df.at[row,'OCIM Direction'] = 'Lend'
            repo_report_df.at[row,'Dealer Side'] = 'Bid'
        if((repo_report_df.at[row,'OCIM Direction'] == 'Borrow' and repo_report_df.at[row,'Notional Quantity'] >= 0) or 
           (repo_report_df.at[row,'OCIM Direction'] == 'Lend' and repo_report_df.at[row,'Notional Quantity'] < 0)):
            overborrowed_df = pd.concat([overborrowed_df,pd.DataFrame(repo_report_df.loc[row,:]).T])
overborrowed_df = overborrowed_df[overborrowed_df['Total Amount to Show'] != 0]
overborrowed_df.pop('OCIM Direction')

### ---------------------------------------------------- OPEN COUNTERPARTY OPTIONS ---------------------------------------------------- ###

### OPEN ENDED TRADES ###
unique_open_ended_bonds = open_ended_repo_trades['Description'].unique()
unique_ctpys = open_ended_repo_trades['Counterparty'].unique()
open_ctpy_df = pd.DataFrame()
open_ctpy_df.index = unique_open_ended_bonds
open_ctpy_df[unique_ctpys] = np.nan

### CALCULATE NOTIONALS ###
for bond in range(0,len(unique_open_ended_bonds)):
    each_bond = unique_open_ended_bonds[bond]
    bond_subset = open_ended_repo_trades[open_ended_repo_trades['Description'] == each_bond]
    ctpy_subset_unique = bond_subset['Counterparty'].unique()
    if(len(bond_subset) == 1 and len(ctpy_subset_unique) == 1):
        open_ctpy_df.loc[each_bond,ctpy_subset_unique[0]] = float(bond_subset['Notional Quantity'])
    elif(len(bond_subset) > 1 and len(ctpy_subset_unique) == 1):
        notional_sum = sum(bond_subset['Notional Quantity'])
        open_ctpy_df.loc[each_bond,ctpy_subset_unique[0]] = notional_sum
    elif(len(bond_subset) > 1 and len(ctpy_subset_unique) > 1):
        for x in range(0,len(ctpy_subset_unique)):
            notional_sum = sum(bond_subset[bond_subset['Counterparty'] == ctpy_subset_unique[x]]['Notional Quantity'])
            open_ctpy_df.loc[each_bond,ctpy_subset_unique[x]] = notional_sum

open_ctpy_df = open_ctpy_df.fillna(0)
open_ctpy_df['Grand Total'] = open_ctpy_df.sum(axis = 1)

### FIND OPEN TRADES WE CAN CLOSE ###
open_ctpy_df.index = [x[7:] for x in open_ctpy_df.index]
open_ctpy_options = pd.DataFrame([open_ctpy_df.iloc[row,:] for row in range(0,len(open_ctpy_df)) if open_ctpy_df.index[row] in overborrowed_df.index])
open_ctpy_options.insert(0,'Bonds',open_ctpy_options.index)

### REFORMAT NUMBERS ###
for col in range(1,len(open_ctpy_options.columns)):
    for row in range(0,len(open_ctpy_options.index)):
        open_ctpy_options.iloc[row,col] = ('{:,}'.format(int(open_ctpy_options.iloc[row,col])))

### EXPORT AS EXCEL ###
today_date = date.today()
date_string = today_date.strftime("%m%d%Y")
excel_export_string = (r"C:\Users\ChenChen\OC Investment Management, LP\Trading - General\Chen\US Repo Reports\US Repo Trading" 
                       + " " + date_string + ".xlsx")
overborrowed_df.to_excel(excel_export_string)

### REFORMAT NUBMERS ###
for row in overborrowed_df.index:
    overborrowed_df.loc[row,'Notional Quantity'] = ('{:,}'.format(int(overborrowed_df.at[row,'Notional Quantity'])))
    overborrowed_df.loc[row,'Total Amount to Show'] = ('{:,}'.format(int(overborrowed_df.at[row,'Total Amount to Show'])))

### ----------------------------------------------------------- SEND EMAIL ----------------------------------------------------------- ###

### CUSTOM FUNCTIONS ###    
def create_email(input_df1:pd.DataFrame,input_df2:pd.DataFrame) -> str:
    return  f''' 
    <b> Table 1: Overborrowed Bonds
    {build_table(input_df1,
                 'red_light',
                 font_size='13px',
                 text_align='center',
                 width = '400px')}
    <b> Table 2: Open Trade Options to Close Based on Overborrowed Bonds
    <b> (If nothing is displayed, no open trade options available to reconcile overborrowed bonds)
    {build_table(input_df2,
                 'red_light',
                 font_size='13px',
                 text_align='center',
                 width = '400px')}
    '''

### EMAIL ###
exec(open(r'C:\Users\ChenChen\OneDrive - OC Investment Management, LP\Documents\GitHub\Chen\Enfusion-Ops\EmailClient.py').read())
ABS_PATH = os.path.dirname(os.path.abspath(excel_export_string)) # get path of file
email = EmailClient()
email.send_test_email(str('US Repo Report ' + date_string),create_email(overborrowed_df,open_ctpy_options))
