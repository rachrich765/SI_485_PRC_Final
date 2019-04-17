from functions import *
from clean_new import *
from clean_all import *
from add_info import *
import numpy as np

added_data = pd.read_csv('data_breach_chronology_new.csv')
cleaned_new_df = clean_new_data(added_data)
type1_list = get_breach_type_cause(cleaned_new_df)
type2_list = get_breach_type_classifier(cleaned_new_df)
cleaned_new_df['Type of Breach'] = final_list(type1_list, type2_list)
cleaned_new_df = cleaned_new_df.drop(columns = ['PDF text'])

#final_df = final_df.drop(columns = ['Entity Type', 'Details', 'Link'])

# export data breach chronology and update the recent file
final_df.to_csv('data_breach_chronology_new.csv', index = False)
#recent.to_csv('recent.csv')
