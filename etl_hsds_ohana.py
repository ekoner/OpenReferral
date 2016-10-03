import os
import re
import sys
import json
import pandas as pd
import numpy as np
import argparse as ap
from time import strftime

# Initialise
parser = ap.ArgumentParser()
parser.add_argument("-f1", "--hsds", help="HSDS Folder Path")
args = parser.parse_args()

print( "hsds {}".format(
        args.hsds))

def main():
    """
        Purpose: A hastily put together script to convert HSDS (Open Referral) files to Ohana.
        Status:  Needs work.
        Author:  Edafe Onerhime - Open Data Services
        Date:    25.08.2016
    """    
    os.system('clear')
    print('*********************************************')
    print('Convert HSDS format csv files to Ohana format')
    print('*********************************************')
    print('')
    print('Python version:   ' + sys.version)
    print('Pandas version:   ' + pd.__version__) 
    print('Numpy version:    ' + np.version.version)
    print('ArgParse version: ' + ap.__version__)       
    print('')
    print('Parameters provided:')
    print('HSDS Folder Path: ', args.hsds)    
    print('')

####################
### Check Params ###
####################   
    if not os.path.isdir(str(args.hsds)):
        print(strftime('%X'), 'Provide a valid HSDS folder')
        return       
    fileList = [name for name in os.listdir(args.hsds) if os.path.isfile(os.path.join(args.hsds,name)) and os.path.splitext(os.path.join(args.hsds,name))[1] == '.csv']  
    if len(fileList) == 0:
        print(strftime('%X'), 'HSDS folder is empty')
        return       

    if not os.path.exists(os.path.join(args.hsds,'ohana')):
        os.makedirs(os.path.join(args.hsds,'ohana'))

####################
### Convert Orgs ###
#################### 
    print(strftime('%X'), 'Records Loaded')
    fileName = "organizations.csv"
    print(strftime('%X'), 'Processing Organization Records')
    if fileName in fileList:
        df = pd.read_csv(os.path.join(args.hsds,fileName))
        
        # Rename url->website, year_incorporated->date_incorporated
        df.rename(columns={'url':'website', 'year_incorporated':'date_incorporated'}, inplace=True)
        
        # Add legal_status, accreditations, funding_sources,licenses
        df["legal_status"] = ""
        df["accreditations"] = ""
        df["funding_sources"] = ""
        df["licenses"] = ""
        
        # Rearrange columns
        df = df[["id", "name", "alternate_name", "description", "email", "website", "legal_status", "tax_status", "tax_id", "date_incorporated", "accreditations", "funding_sources", "licenses"]]
        df.to_csv(os.path.join(args.hsds,'ohana', fileName),index=False,na_rep="")          
        print(strftime('%X'), 'Organization Records:  ', "{:,}".format(df.shape[0]))

    fileName = "services.csv"        
    print(strftime('%X'), 'Processing Services Records')    
    if fileName in fileList:
        df = pd.read_csv(os.path.join(args.hsds,fileName))
        
        # Remove Organization ID
        del df["organization_id"]
        
        # Rename url->website
        df.rename(columns={'url':'website'}, inplace=True)
        
        # Add new columns
        df["accepted_payments"] = ""
        df["eligibility"] = ""
        df["email"] = ""
        df["fees"] = ""
        df["funding_sources"] = ""
        df["interpretation_services"] = ""
        df["keywords"] = ""
        df["languages"] = ""
        df["required_documents"] = ""
        df["service_areas"] = ""
        df["taxonomy_ids"] = ""
        
        # Rearrange columns
        df = df[["id", "program_id", "location_id", "name", "alternate_name", "description", "website", "email", "status", "application_process", "wait_time", "accepted_payments", "eligibility", "email", "fees", "funding_sources", "interpretation_services", "keywords", "languages", "required_documents", "service_areas", "taxonomy_ids"]]
        df.to_csv(os.path.join(args.hsds,'ohana',fileName),index=False,na_rep="")          
        print(strftime('%X'), 'Service Records:  ', "{:,}".format(df.shape[0]))

    fileName = "locations.csv"        
    print(strftime('%X'), 'Processing Locations Records')        
    if fileName in fileList:
        df = pd.read_csv(os.path.join(args.hsds,fileName))

        # Add new columns
        df["accessibility"] = ""
        df["admin_emails"] = ""
        df["email"] = ""
        df["languages"] = ""
        df["short_desc"] = ""
        df["website"] = ""
        df["virtual"] = "FALSE"

        # Rearrange columns
        df = df[["id", "organization_id", "name", "alternate_name", "transportation", "latitude", "longitude", "description", "accessibility", "admin_emails", "email", "languages", "short_desc", "website", "virtual"]]        
        df.to_csv(os.path.join(args.hsds,'ohana',fileName),index=False,na_rep="")                      
        print(strftime('%X'), 'Location Records:  ', "{:,}".format(df.shape[0]))
        
    fileName = "phones.csv"
    print(strftime('%X'), 'Processing Phone Records')            
    if not fileName in fileList and "organizations.csv" in fileList:
        df = pd.read_csv(os.path.join(args.hsds,"organizations.csv"), usecols=["id"])
        
        # Add columns
        df["location_id"] = ""
        df["contact_id"] = ""
        df["organization_id"] = df["id"]
        df["service_id"] = ""
        df["number"] = "000-000-0000"
        df["vanity_number"] = ""
        df["extension"] = ""
        df["department"] = ""
        df["number_type"] = "voice"
        df["country_prefix"] = ""
        df.to_csv(os.path.join(args.hsds,'ohana',fileName),index=False,na_rep="")                      
        print(strftime('%X'), 'Phone Records:  ', "{:,}".format(df.shape[0]))

    fileName = "addresses.csv"
    print(strftime('%X'), 'Processing Address Records')                
    if not fileName in fileList and "locations.csv" in fileList:
        df = pd.read_csv(os.path.join(args.hsds,"locations.csv"), usecols=["id"])

        df["location_id"] = df["id"]
        df["address_1"] = "-Placeholder-"
        df["address_2"] = "-Placeholder-"
        df["city"] = "Miami"
        df["state_province"] = "FL"
        df["postal_code"] = "33143"
        df["country"] = "US"
        df.to_csv(os.path.join(args.hsds,'ohana',fileName),index=False,na_rep="")                      
        print(strftime('%X'), 'Address Records:  ', "{:,}".format(df.shape[0]))
        
    print(strftime('%X'), 'Complete')    
if __name__ == "__main__":
    main()
