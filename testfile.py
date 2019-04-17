from functions import *
from clean_new import *
from clean_all import *
from add_info import *
import numpy as np

cleaned_new_df = pd.read_csv('test_new.csv')
all_data = cleaned_new_df
final_df = clean_ents(all_data)
print(final_df['Name of Entity'])
final_df = remove_duplicates(final_df)
#final_df = final_df.drop(columns = ['Entity Type', 'Details', 'Link'])

# export data breach chronology and update the recent file
final_df.to_csv('data_breach_chronology_new.csv', index = False)
#recent.to_csv('recent.csv')
