import requests
import json
import os
import pandas as pd
import re

#USER_FOLDER='./'
USER_FOLDER = os.getenv('FINANCE_API_FILES')
TOKEN = os.getenv("DEMO_API_KEY", "blank")
fundsFile = 'Watershed funds for import.xlsx'
assetsFile = 'Watershed assets for import.xlsx'
annualDataFile = 'Watershed annual data for import.xlsx'
holdingsFile = 'Watershed asset holdings for import.xlsx'
#all files need to be Excel with a sheet called "For import" as per the downloadable templates

print(USER_FOLDER)
print(TOKEN)

limit = '10000'
headers = {
    'accept': 'application/json',
    'content-type': 'application/json',
    'authorization': 'Bearer ' + TOKEN,
}


def fullurl(url):
    return 'https://api.watershedclimate.com/' + url


def clean_numeric_string(s):
    if pd.isna(s):
        return s
    s = str(s).strip()  # Convert to string and remove whitespace
    s = s.replace(',', '')  # Remove commas (for some european formats, replace with a period may be useful
    s = re.sub(r'[^\d.-]', '', s)  # Remove other non-numeric characters
    return s if s else None  # Return None for empty strings


def replace_empty_values(s):
    if pd.isna(s):
        s = 'Overall'
    return s

def replace_empty_attributionfactor1(n):
    if pd.isna(n):
        n=1
    return n

def get(url):
    print("GET query to " + fullurl(url))
    request = requests.get(fullurl(url), headers=headers)
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return request.json()


def delete(url):
    print("DELETE query to " + fullurl(url))
    request = requests.delete(fullurl(url), headers=headers)
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return


def post(url, body):
    print("")
    print("POST query to " + fullurl(url))
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
                if key == 'id' and action == 'delete':
                    delete('v1/finance/' + endpoint + value)
    elif isinstance(json_obj, list):
        for item in json_obj:
            iterate_json(item, action, endpoint)
    else:
        key = json_obj.items
        if key == 'id' and action == 'delete':
            delete('v1/finance/funds' + endpoint + json_obj)
            print("deleted fund " + json_obj)


def iterate_json_return_id(json_obj, assets_ids_list, assets_names_list, key_to_check, value_to_get):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if isinstance(value, (dict, list)):
                iterate_json_return_id(value, assets_ids_list, assets_names_list, key_to_check, value_to_get)
            else:
                if key == key_to_check:
                    assets_ids_list.append(value)
                if key == value_to_get:
                    assets_names_list.append(value)
    elif isinstance(json_obj, list):
        for item in json_obj:
            iterate_json_return_id(item, assets_ids_list, assets_names_list, key_to_check, value_to_get)
    else:
        key = json_obj.items
        if key == key_to_check:  #don't think it ever gets here as lowest level element in our retrieved json is a list so handled above
            return


def get_asset_id(assetlist, searchterm):
    for assetrecord in assetlist:
        if searchterm == assetrecord['name']:
            return assetrecord['id']


def rename_scope_overrides(data_df):
    #custom handling for any columns that don't adhere to a common pattern (including 3.05 and 3.11 !)
    sc2_col1 = 'Scope 2, location-based override (tCO₂e) [Optional]'
    sc2_col2 = 'Scope 2, location-based override PCAF score[Optional]'
    sc2_col3 = 'Scope 2, market-based override (tCO₂e) [Optional]'
    sc2_col4 = 'Scope 2, market-based override PCAF score [Optional]'
    sc3_col35 = 'Scope 3.5 override (tCO2e) [Optional]'
    sc3_col311 = 'Scope 3.11 override (tCO2e) [Optional]'
    sc2_api1 = 'scope2LocationOverrideTco2e'
    sc2_api2 = 'scope2LocationOverridePcaf'
    sc2_api3 = 'scope2MarketOverrideTco2e'
    sc2_api4 = 'scope2MarketOverridePcaf'
    sc3_api35 = 'scope305OverrideTco2e'
    sc3_api311 = 'scope311OverrideTco2e'
    sc3_type = 'Scope 3 override kind [Required if a Scope 3 or Sub-scope 3 override is provided]'
    sc3_type_api = 'scope3OverrideKind'
    data_df.rename(columns={sc2_col1: sc2_api1, sc2_col2: sc2_api2, sc2_col3: sc2_api3, sc2_col4: sc2_api4, sc3_col35: sc3_api35, sc3_col311: sc3_api311,
                            sc3_type: sc3_type_api}, inplace=True)

    for scopenum in (1, 3):
        column_name = 'Scope ' + str(scopenum) + ' override (tCO₂e) [Optional]'
        API_column_name = 'scope' + str(scopenum) + 'Override' + 'Tco2e'
        PCAF_column_name = 'Scope ' + str(scopenum) + ' override PCAF score [Optional]'
        PCAF_API_column_name = 'scope' + str(scopenum) + 'OverridePcaf'
        if column_name in data_df.columns:
            data_df.rename(columns={column_name: API_column_name}, inplace=True)
        if PCAF_column_name in data_df.columns:
            data_df.rename(columns={PCAF_column_name: PCAF_API_column_name}, inplace=True)
    for subscopenum in range(1, 16):
        if subscopenum <10:
            subscopetext = '0' + str(subscopenum)
        else:
            subscopetext = str(subscopenum)
        column_name = 'Scope 3.' + str(subscopenum) + ' override (tCO₂e) [Optional]'
        api_column_name = 'scope3' + subscopetext + 'Override' + 'Tco2e'
        if column_name in data_df.columns:
            data_df.rename(columns={column_name: api_column_name}, inplace=True)


#First, retrieve the ids of all existing funds in order to DELETE all of them
print('retrieving list of current Funds to delete')
Funds = get(f'v1/finance/funds?limit=' + limit)
iterate_json(Funds, 'delete', 'funds/')
print(f'posting new funds list from ', USER_FOLDER + fundsFile)

#Second, POST new funds from excel files
funds_df = pd.read_excel(USER_FOLDER + fundsFile, sheet_name='For import')

if 'Fund name [Required]' in funds_df.columns:
    funds_df.rename(columns={'Fund name [Required]': 'name'}, inplace=True)
if 'Fund group [Optional]' in funds_df.columns:
    funds_df.rename(columns={'Fund group [Optional]': 'fundGroup'}, inplace=True)
if 'Fund category [Optional]' in funds_df.columns:
    funds_df.rename(columns={'Fund category [Optional]': 'fundCategory'}, inplace=True)

funds_data_json = json.loads(funds_df.to_json(orient="records"))
for record in funds_data_json:
    print(record)
    post('v1/finance/funds', record)

#Third, do the same to DELETE all existing assets
print('retrieving list of current Assets to delete')
assets_json = get(f'v1/finance/asset-corporates?limit=' + limit)
iterate_json(assets_json, 'delete', 'asset-corporates/')

#Fourth, POST new Corporate Assets from excel
assets_df = pd.read_excel(USER_FOLDER + assetsFile, sheet_name='For import')
if 'Asset name [Required]' in assets_df.columns:
    assets_df.rename(columns={'Asset name [Required]': 'name'}, inplace=True)
if 'Currency [Required]' in assets_df.columns:
    assets_df.rename(columns={'Currency [Required]': 'currencyCode'}, inplace=True)
if 'Industry [Required to estimate total emissions for corporate assets]' in assets_df.columns:
    assets_df.rename(columns={'Industry [Required to estimate total emissions for corporate assets]': 'naicsCode'},
                     inplace=True)
assets_df['naicsCode'] = assets_df['naicsCode'].astype('string')  #numeric naicsCodes need to be strings for API
if 'Country [Required]' in assets_df.columns:
    assets_df.rename(columns={'Country [Required]': 'countryAlpha2'}, inplace=True)
print(f'posting new assets list from ', USER_FOLDER + assetsFile)
assets_data_json = json.loads(assets_df.to_json(orient="records"))
for record in assets_data_json:
    print(record)
    post('v1/finance/asset-corporates', record)

#Fifth, DELETE all existing Annual Data (should already by empty thanks to asset deletion)
assets_years_json = get(f'v1/finance/asset-years?limit=' + limit)
iterate_json(assets_years_json, 'delete', 'asset-years/')

#Sixth, POST new Annual Data from excel - this is more complex than the previous two uploads because we have to find out and match the asset IDs created automatically during step 4 above, we can't just use their names like IDI
annual_data_df = pd.read_excel(USER_FOLDER + annualDataFile, sheet_name='For import')
#rename pretty much every column because the excel and api use different naming
if 'Asset name [Required]' in annual_data_df.columns:
    assets_df.rename(columns={'Asset name [Required]': 'name'}, inplace=True)
if 'Currency [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Currency [Required]': 'currencyCode'}, inplace=True)
if 'Year [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Year [Required]': 'year'}, inplace=True)
if 'Asset value [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Asset value [Required]': 'valueNative'}, inplace=True)
if 'Asset value [Required to calculate financed emissions for all asset classes but commercial lines of insurance]' in annual_data_df.columns:
    annual_data_df.rename(columns={
        'Asset value [Required to calculate financed emissions for all asset classes but commercial lines of insurance]': 'valueNative'},
                          inplace=True)
rename_scope_overrides(annual_data_df)

annual_data_df['valueNative'] = annual_data_df['valueNative'].apply(clean_numeric_string)
annual_data_df['valueNative'] = annual_data_df['valueNative'].astype('float')
annual_data_df['scope3OverrideKind'] = annual_data_df['scope3OverrideKind'].apply(replace_empty_values)

#This requires the assetID but only contains the asset name. IDI does this matching for us but the API doesn't, so need to retrieve the full list of assets first
assets_json = get('v1/finance/asset-corporates?limit=' + limit)
assets_ids_list = []
assets_names_list = []  # set up a new dataframe to populate with asset ids and another list to hold the matching names
iterate_json_return_id(assets_json, assets_ids_list, assets_names_list, 'id',
                       'name')  #use a tweaked iterate json function to populate the two aligned lists
#merge the two columns together into a new 2 column dataframe which will be used to lookup on name in order to upload the right asset id (kind of AAF but that's what the API needs)
asset_lookups_df = pd.DataFrame({'assetCorporateId': assets_ids_list, 'name': assets_names_list})

# join data returned from REST call with new annual data for assets Using the name of the asset as join index
if 'Asset name [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Asset name [Required]': 'Asset'}, inplace=True)
if 'Revenue [Required for corporate assets]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Revenue [Required for corporate assets]': 'revenueNative'}, inplace=True)
merged_annual_data_df = pd.merge(annual_data_df, asset_lookups_df, left_on='Asset', right_on='name', how='left')
#iterate through joined data, submitting new annual data one asset at a time
print(f'Posting new annual data from join of retrieved assets and ', USER_FOLDER + annualDataFile)
annual_data_json = json.loads(merged_annual_data_df.to_json(orient="records"))
for record in annual_data_json:
    print(record)
    post('v1/finance/asset-years', record)

#Seventh: delete any Holdings data (should also already by empty thanks to asset deletion)
print('Getting existing list of holdings to delete')
Holdings = get(f'v1/finance/asset-holdings?limit=' + limit)
iterate_json(Holdings, 'delete', 'asset-holdings/')

#Eighth and final step - POST new Holdings Data from csv - customisation here for e.g. LTV percentages instead of absolutes, would require retrieval of annual data first to get values
#This requires building a table for upload that joins assetyear (which does not include name!) and assets (which does)
print('Get Asset Year IDs') #IDs for new Annual Data (assetYears) will have been generated by the previous step so we need to query them
asset_years_data_json = get(f'v1/finance/asset-years?limit=' + limit)
assets_ids_list = []
assets_names_list = []  # set up a new dataframe to populate with assetyear ids and another list to hold the matching names - this is the same technique we used for assets
iterate_json_return_id(asset_years_data_json, assets_ids_list, assets_names_list, 'id',
                       'assetCorporateId')  #use a tweaked iterate json function to populate the two lists
annual_data_df = pd.DataFrame({'assetYearId': assets_ids_list, 'Asset': assets_names_list})
merged_annual_data_df=pd.merge(merged_annual_data_df,annual_data_df,left_on='assetCorporateId',right_on='Asset', how='left')

# join data returned from REST call with new annual data (also from REST call for this holdings buildout), using the name of the asset as join index
annual_data_assets_lookup_df = pd.merge(merged_annual_data_df, asset_lookups_df, left_on='assetCorporateId', right_on='assetCorporateId', how='left')

print(f'Reading New Holdings from file ', USER_FOLDER + holdingsFile)
annual_holdings_df = pd.read_excel(USER_FOLDER + holdingsFile, sheet_name='For import')
# join data returned from REST calls with new annual holdings for assets Using the name of the asset as join index and bring in fund ID and asset year ID - full version would need to cater for multiple years by matching on both assetyear IDs and year
if 'Asset name [Required]' in annual_holdings_df:
    annual_holdings_df.rename(columns={'Asset name [Required]': 'Asset'}, inplace=True)
holdings_merged_annual_data_df = pd.merge(annual_data_assets_lookup_df, annual_holdings_df, left_on='name_x', right_on='Asset',
                                          how='left')
# Specific example handling for a demo, will not be applied to standard template data
if 'Attribution Factor 1 - Ownership' in holdings_merged_annual_data_df.columns:
    holdings_merged_annual_data_df['Attribution Factor 1 - Ownership'] =  holdings_merged_annual_data_df['Attribution Factor 1 - Ownership'].apply(replace_empty_attributionfactor1)
    holdings_merged_annual_data_df["outstandingAmountNative"]=holdings_merged_annual_data_df['valueNative']*holdings_merged_annual_data_df["Attribution Factor 1 - Ownership"]*holdings_merged_annual_data_df["Attribution Factor 2 - LTV"]


# add the fund id last once you get holdings data which includes Fund name!
# join funds data returned from REST call with existing asset & annual data created above
print('Retrieving funds to provide IDs for upload')
funds = get('v1/finance/funds?limit=' + limit)
fund_ids_list = []
fund_names_list = []
iterate_json_return_id(funds, fund_ids_list, fund_names_list, 'id',
                       'name')  #use a tweaked iterate json function to populate the two lists
funds_data_df = pd.DataFrame(
    {'fundId': fund_ids_list, 'FundName': fund_names_list})  #we use this later after we read in the holdings
if 'Fund name [Required]' in holdings_merged_annual_data_df:
    holdings_merged_annual_data_df.rename(columns={'Fund name [Required]': 'Fund name'}, inplace=True)
if not('outstandingAmountNative' in holdings_merged_annual_data_df):
    holdings_merged_annual_data_df.rename(columns={'Outstanding amount [Optional]': 'outstandingAmountNative'},
                                          inplace=True)
if 'Outstanding amount Q1 [Optional]' in holdings_merged_annual_data_df:
    holdings_merged_annual_data_df.rename(columns={'Outstanding amount Q1 [Optional]': 'outstandingAmountNativeQ1'},
                                          inplace=True)
if 'Outstanding amount Q2 [Optional]' in holdings_merged_annual_data_df:
    holdings_merged_annual_data_df.rename(columns={'Outstanding amount Q2 [Optional]': 'outstandingAmountNativeQ2'},
                                          inplace=True)
if 'Outstanding amount Q3 [Optional]' in holdings_merged_annual_data_df:
    holdings_merged_annual_data_df.rename(columns={'Outstanding amount Q3 [Optional]': 'outstandingAmountNativeQ3'},
                                          inplace=True)
if 'Outstanding amount Q4 [Optional]' in holdings_merged_annual_data_df:
    holdings_merged_annual_data_df.rename(columns={'Outstanding amount Q4 [Optional]': 'outstandingAmountNativeQ4'},
                                          inplace=True)

funds_merged_annual_data_df = pd.merge(holdings_merged_annual_data_df, funds_data_df, left_on='Fund name',
                                       right_on='FundName', how='left')
funds_merged_annual_data_df.rename(columns={'Asset class [Required]':'assetClass'}, inplace=True)
funds_merged_annual_data_df.drop(columns=['Currency [Required]','Asset_x','FundName','Watershed Asset Holding ID [Required for reupload, do not change]'], inplace=True)
#print (funds_merged_annual_data.columns)
print('Inserting Holdings records')
holdings_data_json = json.loads(funds_merged_annual_data_df.to_json(orient="records"))
for record in holdings_data_json:
    print(record)
    post('v1/finance/asset-holdings', record)
print('Upload Successful')
