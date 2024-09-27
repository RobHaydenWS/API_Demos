
import requests
import json
import os
import pandas as pd
import re

#USER_FOLDER='./'
USER_FOLDER=os.getenv('FINANCE_API_FILES')
TOKEN = os.getenv("DEMO_API_KEY", "blank")
fundsFile = 'Funds.xlsx'
assetsFile = 'assets sample.xlsx'
annualDataFile = 'annual data sample.xlsx'
holdingsFile = 'asset holdings sample.xlsx'
#all files need to be Excel with a sheet called "for Import" as per the downloadable templates

print (USER_FOLDER)
print (TOKEN)

limit='10000'
def fullurl(url):
    return 'https://api.watershedclimate.com/' +url

headers = {
  'accept': 'application/json',
  'content-type': 'application/json',
  'authorization': 'Bearer '+TOKEN,
}

def clean_numeric_string(s):
    if pd.isna(s):
        return s
    s = str(s).strip()  # Convert to string and remove whitespace
    s = s.replace(',', '')  # Remove commas
    s = re.sub(r'[^\d.-]', '', s)  # Remove other non-numeric characters
    return s if s else None  # Return None for empty strings

def replace_empty_values(s):
    if pd.isna(s):
            s='Overall'
    return s

def get(url):
    print("GET query to "+fullurl(url))
    request = requests.get(fullurl(url), headers=headers)
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return request.json()

def delete(url):
    print("DELETE query to "+fullurl(url))
    request = requests.delete(fullurl(url), headers=headers)
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return


def post(url, body):
    print("")
    print("POST query to "+fullurl(url))
    request = requests.post(fullurl(url), headers=headers, data=json.dumps(body))
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return request.json()

def iterate_json(json_obj, action, endpoint):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if isinstance(value, (dict, list)):
                iterate_json(value, action, endpoint)
            else:
                if key=='id' and action=='delete':
                    delete('v1/finance/'+endpoint+value)
    elif isinstance(json_obj, list):
        for item in json_obj:
            iterate_json(item, action, endpoint)
    else:
        key=json_obj.items
        if key == 'id' and action == 'delete':
            delete('v1/finance/funds'+endpoint+json_obj)
            print("deleted fund "+json_obj)

def iterate_json_return_id(json_obj, assets_ids_list, assets_names_list, key_to_check, value_to_get):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if isinstance(value, (dict, list)):
                iterate_json_return_id(value, assets_ids_list, assets_names_list, key_to_check, value_to_get)
            else:
                if key==key_to_check:
                    assets_ids_list.append(value)
                if key==value_to_get:
                    assets_names_list.append(value)
    elif isinstance(json_obj, list):
        for item in json_obj:
            iterate_json_return_id(item, assets_ids_list, assets_names_list, key_to_check, value_to_get)
    else:
        key=json_obj.items
        if key == key_to_check: #don't think it ever gets here as lowest level element in our retrieved json is a list so handled above
            return

def get_asset_id(assetlist, searchterm):
    for assetrecord in assetlist:
        if searchterm==assetrecord['name']:
            return assetrecord['id']

def rename_scope_overrides(data_df):
    sc2_col1='Scope 2, location-based override (tCO₂e) [Optional]'
    sc2_col2='Scope 2, location - based override PCAF score[Optional]'
    sc2_col3='Scope 2, market-based override (tCO₂e) [Optional]'
    sc2_col4='Scope 2, market-based override PCAF score [Optional]'
    sc2_api1='scope2LocationOverrideTco2e'
    sc2_api2='scope2LocationOverridePcaf'
    sc2_api3='scope2MarketOverrideTco2e'
    sc2_api4='scope2MarketOverridePcaf'
    sc3_type = 'Scope 3 override kind [Required if a Scope 3 or Sub-scope 3 override is provided]'
    sc3_type_api = 'scope3OverrideKind'
    data_df.rename(columns={sc2_col1: sc2_api1, sc2_col2: sc2_api2, sc2_col3: sc2_api3, sc2_col4: sc2_api4, sc3_type:sc3_type_api}, inplace=True)

    for scopenum in (1,3):
        column_name='Scope '+str(scopenum)+' override (tCO₂e) [Optional]'
        API_column_name='scope'+str(scopenum)+'Override'+'Tco2e'
        PCAF_column_name='Scope '+str(scopenum)+' override PCAF score [Optional]'
        PCAF_API_column_name='scope'+str(scopenum)+'OverridePcaf'
        if column_name in data_df.columns:
            data_df.rename(columns={column_name:API_column_name}, inplace=True)
        if PCAF_column_name in data_df.columns:
            data_df.rename(columns={PCAF_column_name:PCAF_API_column_name}, inplace=True)
    for subscopenum in range (1,15):
        if subscopenum<10:
            subscopetext='0'+str(subscopenum)
        else:
            subscopetext=str(subscopenum)
        column_name = 'Scope 3.' + str(subscopetext) + ' override (tCO₂e) [Optional]'
        API_column_name = 'scope3' + str(subscopetext) + 'Override' + 'Tco2e'
        if column_name in data_df.columns:
            data_df.rename(columns={column_name: API_column_name}, inplace=True)

#DELETE all existing funds
print ('retrieving list of current Funds to delete')
Funds = get(f'v1/finance/funds?limit='+limit)
iterate_json(Funds,'delete', 'funds/')
print(f'posting new funds list from ',USER_FOLDER+fundsFile)
#POST new funds from csv

funds_df = pd.read_excel(USER_FOLDER+fundsFile, sheet_name='For import')
print (funds_df.columns)

if 'Fund name [Required]' in funds_df.columns:
    funds_df.rename(columns={'Fund name [Required]':'name'}, inplace=True)
if 'Fund group [Optional]' in funds_df.columns:
    funds_df.rename(columns={'Fund group [Optional]':'fundGroup'}, inplace=True)
if 'Fund category [Optional]' in funds_df.columns:
    funds_df.rename(columns={'Fund category [Optional]':'fundCategory'}, inplace=True)

funds_data_json = json.loads(funds_df.to_json(orient="records"))
for record in funds_data_json:
    print(record)
    post('v1/finance/funds',record)

#DELETE all existing assets
print ('retrieving list of current Assets to delete')
assets_json = get(f'v1/finance/asset-corporates?limit='+limit)
iterate_json(assets_json,'delete', 'asset-corporates/')

#POST new Corporate Assets from csv
assets_df = pd.read_excel(USER_FOLDER+assetsFile, sheet_name='For import')
if 'Asset name [Required]' in assets_df.columns:
    assets_df.rename(columns={'Asset name [Required]':'name'}, inplace=True)
if 'Currency [Required]' in assets_df.columns:
    assets_df.rename(columns={'Currency [Required]':'currencyCode'}, inplace=True)
if 'Industry [Required to estimate total emissions for corporate assets]' in assets_df.columns:
    assets_df.rename(columns={'Industry [Required to estimate total emissions for corporate assets]':'naicsCode'}, inplace=True)
assets_df['naicsCode']=assets_df['naicsCode'].astype('string') #numeric naicsCodes need to be strings for API
if 'Country [Required]' in assets_df.columns:
    assets_df.rename(columns={'Country [Required]':'countryAlpha2'}, inplace=True)
print(f'posting new assets list from ',USER_FOLDER+assetsFile)
assets_data_json = json.loads(assets_df.to_json(orient="records"))
for record in assets_data_json:
    print(record)
    post('v1/finance/asset-corporates',record)

#DELETE all existing Annual Data
assets_years_json = get(f'v1/finance/asset-years?limit='+limit)
iterate_json(assets_years_json,'delete', 'asset-years/')

#POST new Annual Data from excel
annual_data_df = pd.read_excel(USER_FOLDER+annualDataFile, sheet_name='For import')
if 'Asset name [Required]' in annual_data_df.columns:
    assets_df.rename(columns={'Asset name [Required]':'name'}, inplace=True)
if 'Currency [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Currency [Required]':'currencyCode'}, inplace=True)
if 'Year [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Year [Required]':'year'}, inplace=True)
if 'Asset value [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Asset value [Required]':'valueNative'}, inplace=True)
rename_scope_overrides(annual_data_df)

annual_data_df['valueNative']=annual_data_df['valueNative'].apply(clean_numeric_string)
annual_data_df['valueNative']=annual_data_df['valueNative'].astype('float')
annual_data_df['scope3OverrideKind']=annual_data_df['scope3OverrideKind'].apply(replace_empty_values)
#This requires the assetID but only contains the asset name. IDI does this matching for us but the API doesn't, so need to retrieve the full list of assets first
assets_json = get('v1/finance/asset-corporates?limit='+limit)
assets_ids_list=[]
assets_names_list=[] # set up a new dataframe to populate with ids and another list to hold the matching names
iterate_json_return_id(assets_json, assets_ids_list, assets_names_list,'id','name') #use a tweaked iterate json function to populate the two aligned lists
#merge the two columns together
asset_lookups_df = pd.DataFrame({'assetCorporateId': assets_ids_list, 'name': assets_names_list})
# there HAS to be an easier way to do the above, just pulling two columns directly out of json nested records?

# join data returned from REST call with new annual data for assets Using the name of the asset as join index
if 'Asset name [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Asset name [Required]':'Asset'}, inplace=True)
merged_annual_data_df = pd.merge(annual_data_df, asset_lookups_df, left_on='Asset', right_on='name', how='left')

#iterate through joined data, submitting new annual data one asset at a time
print(f'Posting new annual data from join of retrieved assets and ',USER_FOLDER+annualDataFile)
annual_data_json = json.loads(merged_annual_data_df.to_json(orient="records"))
for record in annual_data_json:
    print(record)
    post('v1/finance/asset-years',record)

#Finally: delete any Holdings data and upload new data - customise here to apply percentage instead of absolutes, would require retrieval of annual data first to get values
print('Getting existing list of holdings to delete')
Holdings = get(f'v1/finance/asset-holdings?limit='+limit)
iterate_json(Holdings,'delete', 'asset-holdings/')

#POST new Holdings Data from csv
#This requires building a table for upload that includes assetyear (which does not include name!) and assets (which does)

#Pull a list of assets to get the asset IDs in order to match to both years and holdings (adapted to use asset years from REST not file)
#assets = get('v1/finance/asset-corporates?limit='+limit)
#assets_ids_list=[]
#assets_names_list=[] # set up a new dataframe to populate with ids and another list to hold the matching names
#iterate_json_return_id(assets, assets_ids_list, assets_names_list, 'id', 'name') #use a tweaked iterate json function to populate the two lists - key_to_check field may not be needed?
#merge the two columns together
#asset_lookups = pd.DataFrame({'assetCorporateId': assets_ids_list, 'name': assets_names_list})
#NOTE: commented out the 6 lines above as assets and related objects up to asset_lookups should not have changed since annual_data step above.
print('Get Asset Year IDs')
asset_years_data_json = get(f'v1/finance/asset-years?limit='+limit)
assets_ids_list=[]
assets_names_list=[] # set up a new dataframe to populate with ids and another list to hold the matching names
iterate_json_return_id(asset_years_data_json, assets_ids_list, assets_names_list, 'id', 'assetCorporateId') #use a tweaked iterate json function to populate the two lists
#commented out some debugging
#print (f'names',assets_names_list)
#print (f'ids',assets_ids_list)
annual_data_df=pd.DataFrame({'assetYearId':assets_ids_list, 'Asset': assets_names_list})

# join data returned from REST call with new annual data (also from REST call for this holdings buildout), using the name of the asset as join index
merged_annual_data_df = pd.merge(annual_data_df, asset_lookups_df, left_on='Asset', right_on='assetCorporateId', how='left')

print(f'Reading New Holdings from file ',USER_FOLDER+holdingsFile)
annual_holdings_df = pd.read_excel(USER_FOLDER+holdingsFile, sheet_name='For import')
# join data returned from REST calls with new annual holdings for assets Using the name of the asset as join index and bring in fund ID and asset year ID - full version would need to cater for multiple years by matching on both assetyear IDs and year
if 'Asset name [Required]' in annual_holdings_df:
    annual_holdings_df.rename(columns={'Asset name [Required]':'Asset'}, inplace=True)
holdings_merged_annual_data_df =pd.merge(merged_annual_data_df, annual_holdings_df,left_on='name',right_on='Asset',how='left')

### add the fund id last once you get holdings data which includes Fund name!!
# join funds data returned from REST call with existing asset & annual data created above
print ('Retrieving funds to provide IDs for upload')
funds = get('v1/finance/funds?limit='+limit)
fund_ids_list=[]
fund_names_list=[]
iterate_json_return_id(funds, fund_ids_list, fund_names_list, 'id', 'name') #use a tweaked iterate json function to populate the two lists
funds_data_df=pd.DataFrame({'fundId':fund_ids_list, 'FundName': fund_names_list}) #we use this later after we read in the holdings
if 'Fund name [Required]' in holdings_merged_annual_data_df:
    holdings_merged_annual_data_df.rename(columns={'Fund name [Required]':'Fund name'}, inplace=True)
funds_merged_annual_data_df = pd.merge(holdings_merged_annual_data_df,funds_data_df,left_on='Fund name', right_on='FundName', how='left')
funds_merged_annual_data_df.rename(columns={'Year [Required]': 'year', 'Asset class [Required]':'assetClass', 'Currency [Required]':'currencyCode', 'Outstanding amount [Required]':'outstandingAmountNative'}, inplace=True)
funds_merged_annual_data_df.drop(columns=['name','Fund name','FundName','Asset_x','Asset_y'], inplace=True)
#print (funds_merged_annual_data.columns)
print ('Inserting Holdings records')
holdings_data_json = json.loads(funds_merged_annual_data_df.to_json(orient="records"))
for record in holdings_data_json:
    print(record)
    post('v1/finance/asset-holdings',record)
print('Upload Successful')
