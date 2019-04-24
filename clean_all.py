from datetime import datetime
import dateutil.parser
import pandas as pd
import numpy as np
import approxidate
import string
import re

def clean_ents(df_master):
    # Take the 'Name of Entity' column and remove extra characters
    entities = list(df_master["Name of Entity"].fillna("[None Named]"))
    low_ents = [x.lower() for x in entities] #make all lowercase
    low_ents = [re.sub("-databreach","",x) for x in low_ents] #removes -databreach string at end
    low_ents = [re.sub("-securitybreach","",x) for x in low_ents] #removes -securitybreach string at end
    low_ents = [re.sub("-data breach","",x) for x in low_ents] #removes -data breach string at end
    low_ents = [re.sub("data breach","",x) for x in low_ents] #removes data breach string at end
    low_ents = [re.sub("-breach notification","",x) for x in low_ents] #removes -breach notification string at end
    low_ents = [re.sub("^[0-9-]*","",x) for x in low_ents] #removes dates at start of string
    low_ents = [re.sub(",\s{0,}[0-9].*[a-z]{2}\s{1,}[0-9]{5}","",x) for x in low_ents] #removes addresses
    low_ents = [re.sub(";\s{0,}[0-9].*[a-z]{2}\s{1,}[0-9]{5}","",x) for x in low_ents] #removes addresses
    low_ents = [re.sub("\s{0,}p.o. box\s[0-9].*[a-z]{2}\s{1,}[0-9]{5}","",x) for x in low_ents] #removes addresses
    no_space_punc = {}
    for entity in low_ents:
        ent = re.sub("[\s]","",entity) #remove spaces
        ent = re.sub("[^A-z0-9]","",entity) #remove punctuation
        no_space_punc[entity] = ent
    no_space_punc_reversed = inv_map = [(v, k) for k, v in no_space_punc.items()]

    #making a list of duplicates
    dups = []
    simp = [x[0] for x in no_space_punc_reversed]
    for entity in simp:
        if [x[0] for x in no_space_punc_reversed].count(entity)>1:
            dups.append(entity)
    del_list = []
    for dup in dups:
        indices = sorted([i for i, x in enumerate(simp) if x == dup])
        del_list.extend(indices[1:])
    indices = list(set(sorted(del_list)))
    for i in sorted(indices, reverse=True):
        del no_space_punc_reversed [i]
    final_dict = dict(no_space_punc_reversed)
    clean_ents = []
    for entity in low_ents:
        ent = re.sub("[\s]","",entity) #remove spaces
        ent = re.sub("[^A-z0-9]","",entity) #remove punctuation
        clean_ents.append(final_dict[ent])
    clean_ents = [string.capwords(i) for i in clean_ents]
    df_master["Name of Entity"] = clean_ents
    
    return df_master

def remove_duplicates(y):
    data = y

    #This function takes a string returns the number (if any) within it 
    def clean_number(x):
        # only get number of individuals affected
        m = re.search(r"([\d,]+)", str(x))
        if m is not None:
            # strip comma from number
            try:
                return int(re.sub(",", "", m.group(1)))
            except:
                return None
        else:
            return None

    #This creates a list of eevery state in which the breach was reported
    def sum_states(x):
        result = []
        for item in x:
            item = str(item)
            if 'California' in item:
                result.append('California')
            if 'Iowa' in item:
                result.append('Iowa')
            if 'Indiana' in item:
                result.append('Indiana')
            if 'Delaware' in item:
                result.append('Delaware')
            if 'Maine' in item:
                result.append('Maine')
            if 'Maryland' in item:
                result.append('Maryland')
            if 'Montana' in item:
                result.append('Montana')
            if 'New Hampshire' in item:
                result.append('New Hampshire')
            if 'New Jersey' in item:
                result.append('New Jersey')
            if 'Oregon' in item:
                result.append('Oregon')
            if 'Washington' in item:
                result.append('Washington')
            if 'Wisconsin' in item:
                result.append('Wisconsin')
            if 'Vermont' in item:
                result.append('Vermont')
        result = set(result)
        return result

     #This creates a list of eevery state in which the breach was reported
    def sum_urls(x):
        result = []
        for item in x:
            item = str(item)
            if 'https://justice.oregon.gov/consumer/DataBreach/Home/' in item:
                result.append('https://justice.oregon.gov/consumer/DataBreach/Home/')
            if 'https://datcp.wi.gov/Pages/Programs_Services/DataBreaches.aspx' in item:
                result.append('https://datcp.wi.gov/Pages/Programs_Services/DataBreaches.aspx')
            if 'https://ago.vermont.gov/archived-security-breaches/' in item:
                result.append('https://ago.vermont.gov/archived-security-breaches/')
            if 'https://www.atg.wa.gov/data-breach-notifications' in item:
                result.append('https://www.atg.wa.gov/data-breach-notifications')
            if 'https://oag.ca.gov/privacy/databreach/list' in item:
                result.append('https://oag.ca.gov/privacy/databreach/list')
            if 'https://www.in.gov/attorneygeneral/2874.htm' in item:
                result.append('https://www.in.gov/attorneygeneral/2874.htm')
            if 'https://www.iowaattorneygeneral.gov/for-consumers/security-breach-notifications/' in item:
                result.append('https://www.iowaattorneygeneral.gov/for-consumers/security-breach-notifications/')
            if 'https://attorneygeneral.delaware.gov/fraud/cpu/securitybreachnotification/database/' in item:
                result.append('https://attorneygeneral.delaware.gov/fraud/cpu/securitybreachnotification/database/')
            if 'https://www.doj.nh.gov/consumer/security-breaches/' in item:
                result.append('https://www.doj.nh.gov/consumer/security-breaches/')
            if 'https://www.cyber.nj.gov/data-breach-notifications/' in item:
                result.append('https://www.cyber.nj.gov/data-breach-notifications/')
            if 'https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf' in item:
                result.append('https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf')
            if 'https://www.maine.gov/ag/consumer/identity_theft/' in item:
                result.append('https://www.maine.gov/ag/consumer/identity_theft/')
            if 'http://www.marylandattorneygeneral.gov/Pages/IdentityTheft/breachnotices.aspx' in item:
                result.append('http://www.marylandattorneygeneral.gov/Pages/IdentityTheft/breachnotices.aspx')
            if 'https://dojmt.gov/consumer/consumers-known-data-breach-incidents/#' in item:
                result.append('https://dojmt.gov/consumer/consumers-known-data-breach-incidents/#')
        result = set(result)
        return result

    #This function retains the data of duplicate breaches
    def keep_data(x):
        result = ''
        for item in x:
            if item is not None:
                return item 
        else:
            return result

    data['Individuals Affected'] = data['Individuals Affected'].apply(lambda x: clean_number(x))
    data['Start Date of Breach'] = data['Start Date of Breach'].fillna('Not Specified')
    # get all data where Name of Entity and Start Date match, retain the data and return the dataframe.
    dropped_data = data.groupby(["Name of Entity", "Start Date of Breach"]).agg({"Name of Entity":keep_data, "Start Date of Breach":keep_data,
        'Individuals Affected' : np.max, 
        'State Reported' : sum_states, 'Data Stolen': keep_data, 'Date Notice Provided to Consumers': keep_data,
        'Date(s) of Discovery of Breach': keep_data,'Dates of Breach':keep_data,'Industry': keep_data, 
        'Parent Company': keep_data, 'Website': keep_data, 'End Date of Breach': keep_data, 
        'Link to PDF': keep_data, 'Location of Breached Information': keep_data, 
        'Reported Date':keep_data,'Type of Breach': keep_data, 'Source(s)':sum_urls})#'Details': keep_data,

    return dropped_data
    

