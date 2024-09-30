Experimentations and demos using the Watershed API

Extract_Footprint uses the reporting API to download a footprint file and unzip

Finance_Ingestion uses the Finance API to automate the uploading of 4 required files.

All csv files were AI & random number generated and do not represent real organisations. Note: Current version of the finance_ingestion script does NOT use these and uses xlsx files.
API key is set with
export DEMO_API_KEY=
location of data files to import is defined by another env variable
export FINANCE_API_FILES

Reporting API uses a unique footprint id which needs to be retrieved programatically or via https://api-docs.watershed.com/v1/docs/getting-started

For Finance sample files, generic download templates were used but some columns have been renamed to match expected API names so please use these templates. Or if you want to create a PR to use the generic column headings & rename for the REST calls... :)
I've now catered for some standard template column names. There might be some others which need to be picked up, still working on that) 


