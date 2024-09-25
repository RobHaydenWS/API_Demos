
import requests
import json
import os
import pandas as pd

#USER_FOLDER='./'
USER_FOLDER=os.getenv('FINANCE_API_FILES')
TOKEN = os.getenv("DEMO_API_KEY", "blank")
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


df = pd.read_csv(USER_FOLDER+"Funds.csv")

#DELETE all existing funds
print ('retrieving list of current Funds to delete')
Funds = get(f'v1/finance/funds?limit='+limit)
iterate_json(Funds,'delete', 'funds/')
print(f'posting new funds list from ',USER_FOLDER,'funds.csv')
#POST new funds from csv
funds_data = json.loads(df.to_json(orient="records"))
for record in funds_data:
    print(record)
    post('v1/finance/funds',record)

#DELETE all existing assets
print ('retrieving list of current Assets to delete')
Assets = get(f'v1/finance/asset-corporates?limit='+limit)
iterate_json(Assets,'delete', 'asset-corporates/')

#POST new Corporate Assets from csv
df = pd.read_csv(USER_FOLDER+"Assets.csv")
print(f'posting new assets list from ',USER_FOLDER,'Assets.csv')
assets_data = json.loads(df.to_json(orient="records"))
for record in assets_data:
    print(record)
    post('v1/finance/asset-corporates',record)

#DELETE all existing Annual Data
assets = get(f'v1/finance/asset-years?limit='+limit)
iterate_json(assets,'delete', 'asset-years/')

#POST new Annual Data from csv
annual_data_df = pd.read_csv(USER_FOLDER+"AnnualData.csv")
#This requires the assetID but only contains the asset name. IDI does this matching for us but the API doesn't, so need to retrieve the full list of assets first
assets = get('v1/finance/asset-corporates?limit='+limit)
assets_ids_list=[]
assets_names_list=[] # set up a new dataframe to populate with ids and another list to hold the matching names
iterate_json_return_id(assets, assets_ids_list, assets_names_list,'id','name') #use a tweaked iterate json function to populate the two lists
#merge the two columns together
asset_lookups = pd.DataFrame({'assetCorporateId': assets_ids_list, 'name': assets_names_list})
# there HAS to be an easier way to do the above, just pulling two columns out of json nested records?

# join data returned from REST call with new annual data for assets Using the name of the asset as join index
if 'Asset name [Required]' in annual_data_df.columns:
    annual_data_df.rename(columns={'Asset name [Required]':'Asset'}, inplace=True)
merged_annual_data = pd.merge(annual_data_df, asset_lookups, left_on='Asset', right_on='name', how='left')

#iterate through joined data, submitting new annual data one asset at a time
print(f'Posting new annual data from join of retrieved assets and ',USER_FOLDER,'annual_data.csv')
annual_data = json.loads(merged_annual_data.to_json(orient="records"))
for record in annual_data:
    print(record)
    post('v1/finance/asset-years',record)

#Finally: delete any Holdings data and upload new data - customise here to apply percentage instead of absolutes, would require retrieval of annual data first to get values
print('Getting existing list of holdings to delete')
Holdings = get(f'v1/finance/asset-holdings?limit='+limit)
iterate_json(Holdings,'delete', 'asset-holdings/')

#POST new Holdings Data from csv
#This needs building a table for upload that includes assetyear (which does not include name!) and assets (which does)

#Pull a list of assets to get the asset IDs in order to match to both years and holdings (adapted to use asset years from REST not file)
#assets = get('v1/finance/asset-corporates?limit='+limit)
#assets_ids_list=[]
#assets_names_list=[] # set up a new dataframe to populate with ids and another list to hold the matching names
#iterate_json_return_id(assets, assets_ids_list, assets_names_list, 'id', 'name') #use a tweaked iterate json function to populate the two lists - key_to_check field may not be needed?
#merge the two columns together
#asset_lookups = pd.DataFrame({'assetCorporateId': assets_ids_list, 'name': assets_names_list})
#NOTE: commented out the 6 lines above as assets and related objects up to asset_lookups should not have changed since annual_data step above.
print('Get Asset Year IDs')
asset_years_data = get(f'v1/finance/asset-years?limit='+limit)
assets_ids_list=[]
assets_names_list=[] # set up a new dataframe to populate with ids and another list to hold the matching names
iterate_json_return_id(asset_years_data, assets_ids_list, assets_names_list, 'id', 'assetCorporateId') #use a tweaked iterate json function to populate the two lists
#commented out some debugging
#print (f'names',assets_names_list)
#print (f'ids',assets_ids_list)
annual_data_df=pd.DataFrame({'assetYearId':assets_ids_list, 'Asset': assets_names_list})

# join data returned from REST call with new annual data (also from REST call for this holdings buildout), using the name of the asset as join index
merged_annual_data = pd.merge(annual_data_df, asset_lookups, left_on='Asset', right_on='assetCorporateId', how='left')

print(f'Reading New Holdings from file ',USER_FOLDER,'Holdings.csv')
annual_holdings_df = pd.read_csv(USER_FOLDER+"Holdings.csv")
# join data returned from REST calls with new annual holdings for assets Using the name of the asset as join index and bring in fund ID and asset year ID - full version would need to match assetyear IDs on year
if 'Asset name [Required]' in annual_holdings_df:
    annual_holdings_df.rename(columns={'Asset name [Required]':'Asset'}, inplace=True)
holdings_merged_annual_data =pd.merge(merged_annual_data, annual_holdings_df,left_on='name',right_on='Asset',how='left')

### add the fund id last once you get holdings data which includes Fund name!!
# join funds data returned from REST call with existing asset & annual data created above
print ('Retrieving funds to provide IDs for upload')
funds = get('v1/finance/funds?limit='+limit)
fund_ids_list=[]
fund_names_list=[]
iterate_json_return_id(funds, fund_ids_list, fund_names_list, 'id', 'name') #use a tweaked iterate json function to populate the two lists
funds_data_df=pd.DataFrame({'fundId':fund_ids_list, 'FundName': fund_names_list}) #we use this later after we read in the holdings
if 'Fund name [Required]' in holdings_merged_annual_data:
    holdings_merged_annual_data.rename(columns={'Fund name [Required]':'Fund name'}, inplace=True)
funds_merged_annual_data = pd.merge(holdings_merged_annual_data,funds_data_df,left_on='Fund name', right_on='FundName', how='left')
funds_merged_annual_data.rename(columns={'Year [Required]': 'year', 'Asset class [Required]':'assetClass', 'Currency [Required]':'currencyCode', 'Outstanding amount [Required]':'outstandingAmountNative'}, inplace=True)
funds_merged_annual_data.drop(columns=['name','Fund name','FundName','Asset_x','Asset_y'], inplace=True)
#print (funds_merged_annual_data.columns)
print ('Inserting Holdings records')
holdings_data = json.loads(funds_merged_annual_data.to_json(orient="records"))
for record in holdings_data:
    print(record)
    post('v1/finance/asset-holdings',record)
print('Upload Successful')
