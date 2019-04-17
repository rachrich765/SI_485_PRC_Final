import pandas as pd
import string
import re
import approxidate
from datetime import datetime
import dateutil.parser
import numpy as np

#df = pd.read_csv('data_breach_chronology.csv')
#Clean Name of Entity Column
import string
import re

def clean_ents(df_master):
    #df_master = pd.read_csv(csv_file)
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
#     print(len(no_space_punc_reversed))

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

    def clean_number(x):
        # only get number of individuals affected
        m = re.search(r"([\d,]+)", str(x))
        if m is not None:
            #  strip comma from number
            try:
                return int(re.sub(",", "", m.group(1)))
            except:
                return None
        else:
            return None

    def sum_states(x):
        result = []
        for item in x:
            if item not in result:
                result.append(item)
        return result
    def keep_data(x):
        result = ''
        for item in x:
            if item is not None:
                return item 
        else:
            return result

    data['Individuals Affected'] = data['Individuals Affected'].apply(lambda x: clean_number(x))
    print (data['Name of Entity'])
    # get all data where Name of Entity, Start Date, and End Date match
    #duplicates = pd.concat(g for _, g in data.groupby(["Name of Entity", "Start Date of Breach", "End Date of Breach"]))
    dropped_data = data.groupby(["Name of Entity", "Start Date of Breach"]).agg({"Name of Entity":keep_data, "Start Date of Breach":keep_data,
        'Individuals Affected' : np.max, 
        'State Reported' : sum_states, 'Data Stolen': keep_data, 'Date Notice Provided to Consumers': keep_data,
        'Date(s) of Discovery of Breach': keep_data,'Dates of Breach':keep_data,
        'End Date of Breach': keep_data,'Entity Type': keep_data, 
        'Link to PDF': keep_data, 'Location of Breached Information': keep_data, 
        'Reported Date':keep_data,'Type of Breach': keep_data})#'Details': keep_data,'Industry': keep_data, 'Parent Company': keep_data, 'Website': keep_data
    print(dropped_data['Name of Entity'])

    return dropped_data
    

# replce the csv file name with the name of our dataframe
#remove_licates(pd.read_csv('C:/Users/atrm1/Downloads/new_dates.csv'))


