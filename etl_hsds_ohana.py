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

    outputFolder='ohana'
    if not os.path.exists(os.path.join(args.hsds,outputFolder)):
        os.makedirs(os.path.join(args.hsds,outputFolder))

#####################
### Convert Files ###
#####################
    fileNameList = ['Organization','Program','Location','Service','Contact','Phone','Physical_Address']    
    requiredFiles = ['Organization','Location','Service','Phone']
    outputFiles = {'Organization':'organizations.csv','Program':'programs.csv','Location':'locations.csv','Service':'services.csv','Contact':'contacts.csv','Phone':'phones.csv','Physical_Address':'addresses.csv'}
    renameColumns = {'Organization':{'url':'website', 'year_incorporated':'date_incorporated'},
                     'Service':{'url':'website'}}
    removeColumns = {'Service':['organization_id'],'Phone':['type'],'Physical_Address':['attention','address_3','address_4']}
    addtlColumns = {'Organization':['legal_status','accreditations','funding_sources','licenses'], 
                    'Location':['accessibility','admin_emails','alternate_name','description','email','languages','short_desc','website','virtual'],
                    'Service':['accepted_payments','description','eligibility','fees','funding_sources','interpretation_services','keywords','languages','required_documents','service_areas','taxonomy_ids'],
                    'Contact':['location_id'],'Phone':['vanity_number','number_type','country_prefix']}
    fixColumns = {'Organization':{'website':'cleanURL'},'Location':{'description':'setLocDesc','virtual':'setFalse'},'Service':{'website':'cleanURL','description':'setNoneProvided'},'Phone':{'number_type':'setVoice'}}
    reArrangeColumns = {'Organization':['id', 'name', 'alternate_name', 'description', 'email', 'website', 'legal_status', 'tax_status', 'tax_id', 'date_incorporated', 'accreditations', 'funding_sources', 'licenses'], 
                        'Program':['id','organization_id','name','alternate_name'], 
                        'Location':['id','organization_id','accessibility','admin_emails','alternate_name','description','email','languages','latitude','longitude','name','short_desc','transportation','website','virtual'],
                        'Service':['id','location_id','program_id','accepted_payments','alternate_name','application_process','description','eligibility','email','fees','funding_sources','interpretation_services','keywords','languages','name','required_documents','service_areas','status','wait_time','website','taxonomy_ids'],
                        'Contact':['id','location_id','organization_id','service_id','name','title','email','department'],
                        'Phone':['id','location_id','contact_id','organization_id','service_id','number','vanity_number','extension','department','number_type','country_prefix'],
                        'Physical_Address':['id','location_id','address_1','address_2','city','state_province','postal_code','country']}                        
    
    # Fix programs
    cleanURL = lambda x: 'http://' + x if 'http' not in x and len(x.strip()) > 0 else x.strip()
    setLocDesc = lambda x: 'See listed services'
    setNoneProvided = lambda x: 'None Provided'
    setTrue = lambda x: True
    setFalse = lambda x: False    
    setVoice = lambda x: 'voice'
    
    for fileName in fileNameList:
        print(strftime('%X'), 'Processing',fileName,'Records')
        csvFileName = fileName.lower()+'.csv'

        # Check if file exists
        if fileName in requiredFiles and csvFileName not in fileList:
            print(strftime('%X'), csvFileName,'not found and is required')
            return
        
        if csvFileName not in fileList:
            print(strftime('%X'), csvFileName,'not found')
            break    
            
        # Load file        
        df = pd.read_csv(os.path.join(args.hsds,csvFileName))
        print(strftime('%X'), 'Loaded:', "{:,}".format(df.shape[0]),'records')  
        
        # Replace nan with empty string
        df.replace(np.nan,'', regex=True, inplace=True)
        
        # Rename columns 
        if fileName in renameColumns:
            renameCol = renameColumns[fileName]
            df.rename(columns=renameCol, inplace=True) 
            for k,v in renameCol.items():
                print(strftime('%X'), 'Renamed:', k, 'to', v) 

        # Remove columns
        if fileName in removeColumns:
            delColumns = removeColumns[fileName]
            for delCol in delColumns:
                if delCol in df.columns.names:
                    df.drop(delCol, axis=1, inplace=True)
        
        # Add columns (if they don't exist)
        if fileName in addtlColumns:
            newColumns = addtlColumns[fileName]
            for newCol in newColumns:
                if not newCol in df.columns.names:
                    df[newCol] = ""

        # Fix columns                               
        if fileName in fixColumns:
            fixCol = fixColumns[fileName]
            for k,v in fixCol.items():
                if v == 'cleanURL':
                    df[k] = df[k].fillna("").apply(cleanURL)
                if v == 'setTrue':
                    df[k] = df[k].fillna("").apply(setTrue)
                if v == 'setFalse':
                    df[k] = df[k].fillna("").apply(setFalse)
                if v == 'setLocDesc':
                    df[k] = df[k].fillna("").apply(setLocDesc)
                if v == 'setNoneProvided':
                    df[k] = df[k].fillna("").apply(setNoneProvided)  
                if v == 'setVoice':
                    df[k] = df[k].fillna("").apply(setVoice)                                            
                print(strftime('%X'), 'Fixed:', k, 'column using', v) 
                
        # Rearrange columns                
        if fileName in reArrangeColumns:
            reArrangeCol = reArrangeColumns[fileName]
            df = df[reArrangeCol]
            print(strftime('%X'), 'Re-arranged columns') 
            
        # Output file
        if fileName not in outputFiles:
            print(strftime('%X'), 'output file name not found')
            return            
        outFileName = outputFiles[fileName]    
        df.to_csv(os.path.join(args.hsds,outputFolder, outFileName),index=False,na_rep='')          
        print(strftime('%X'), 'Created output file',os.path.join(args.hsds,outputFolder, outFileName)) 
        
    print(strftime('%X'), 'Complete')    
if __name__ == "__main__":
    main()
