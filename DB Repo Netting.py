### ----------------------------------------------------------------- WEEKLY TRADE PULL ----------------------------------------------------------------- ###

### PACKAGES ###
import pandas as pd
from datetime import date
import os         
from xbbg import blp
from pretty_html_table import build_table
import pandas_market_calendars as mcal

### CLIENTS ###
import scripts.clients.enfusion_api_client as enf

### --------------------------------------------------------------------- DATA PULL --------------------------------------------------------------------- ###

### CONNECT TO ENFUSION AND GET FILE ###
client = enf.EnfusionClient('serviceuser@ocimgt.com', 'Enfusion2023!') 
repo_risk_ust = client.get_repo_risk_df()

### DATES ###
today_date = date.today()
today_date_string = today_date.strftime("%m%d%Y")
today_dt = today_date.strftime("%m/%d/%Y")
today_dt_2 = today_date.strftime("%Y-%m-%d")
first_date_of_next_month = today_dt_2[:5] + str(int(today_dt_2[5:7]) + 1) + '-01'
last_date_of_next_month = today_dt_2[:5] + str(int(today_dt_2[5:7]) + 1) + '-31'
nyse = mcal.get_calendar('NYSE')
next_month_trading_days = nyse.schedule(start_date=first_date_of_next_month, end_date=last_date_of_next_month)
first_trade_date = next_month_trading_days.index[0].strftime('%m/%d/%Y')

### FORMAT DATES ###
if(first_trade_date[2:4] == '/0'):
    first_trade_date = first_trade_date[:3] + first_trade_date[4:]
if(first_trade_date[0] == '0'):
    first_trade_date = first_trade_date[1:]
    
### CHANGE FORMAT OF DATES ###
first_date_of_next_month = pd.to_datetime(first_date_of_next_month).strftime('%m/%d/%Y')
if(first_date_of_next_month[2:4] == '/0'):
    first_date_of_next_month = first_date_of_next_month[:3] + first_date_of_next_month[4:]

### SUBSET DATA ###
db_ust = repo_risk_ust[repo_risk_ust['Instrument Type'] == 'Repurchase Agreement']
db_ust = db_ust[db_ust['First Counterparty Name'] == 'Deutsche Bank AG']
db_bond_ust = db_ust[db_ust['UnderlyingInstrumentType'] == 'Bond']
db_bond_ust_mat = db_bond_ust[pd.to_datetime(db_bond_ust['Adjusted Maturity Date']) >= pd.to_datetime(first_trade_date)]
db_bond_ust_settle = db_bond_ust_mat[pd.to_datetime(db_bond_ust_mat['First Settle Date']) < pd.to_datetime(first_date_of_next_month)]

### BDH ###
db_bond_ust_settle['Underlying CUSIP'] = db_bond_ust_settle['Underlying CUSIP'] + ' GOVT'
bbg_bdh = blp.bdp(db_bond_ust_settle['Underlying CUSIP'].unique(),'PX_DIRTY_MID')
db_bond_ust_settle.index = db_bond_ust_settle['Underlying CUSIP']
db_bond_ust_settle = db_bond_ust_settle.join(bbg_bdh)

### NETTING ###
db_bond_ust_settle['dirty_mid_quantity'] = (db_bond_ust_settle['px_dirty_mid'] / 100) * db_bond_ust_settle['Notional Quantity']
db_bond_ust_settle = db_bond_ust_settle.drop('Underlying CUSIP',axis = 1)
db_bond_ust_settle = db_bond_ust_settle.reset_index()

### ABSOLUTE NOTIONAL CALCULATION ###
db_bond_net = pd.pivot_table(db_bond_ust_settle, index=['Adjusted Maturity Date'],values=['dirty_mid_quantity'],aggfunc='sum')
db_net_sum = abs(db_bond_net).sum()
db_bond_net.insert(0,'Counterparty','Deutsche Bank')
db_bond_net.insert(0,'Maturity Date',db_bond_net.index)
email_title = 'Deutsche Bank Notional Netting ' + today_dt + ": $" + "{:,}".format((int(db_net_sum)))

### -------------------------------------------------------- SEND EMAIL -------------------------------------------------------- ###

excel_export_string = (r"C:\Users\ChenChen\OC Investment Management, LP\Trading - General\Chen\Repo Netting" 
                       + "\DB Repo Netting " + today_date_string + ".xlsx")
db_bond_ust_mat.to_excel(excel_export_string)

### CUSTOM FUNCTIONS ###
def create_email(input_df1:pd.DataFrame) -> str:
    return  f''' 
    <b> Table 1: All Appropriate Deutsche Bank Repo Trades
    {build_table(input_df1,
                 'red_light',
                 font_size='15px',
                 text_align='center',
                 width = '1000px')}
    '''

### EMAIL ###
exec(open(r'C:\Users\ChenChen\OneDrive - OC Investment Management, LP\Documents\GitHub\Chen\Enfusion-Ops\EmailClient.py').read())
ABS_PATH = os.path.dirname(os.path.abspath(excel_export_string)) # get path of file
email = EmailClient()
email.send_test_email(email_title,create_email(db_bond_ust_settle[['Current Date','First Counterparty Name','UnderlyingInstrumentType',
                                                                'Underlying CUSIP','Absolute Notional Quantity','First Settle Date',
                                                                'Adjusted Maturity Date','px_dirty_mid']]))