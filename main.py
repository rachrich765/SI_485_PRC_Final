from functions import *
from clean_new import *
from clean_all import *
from add_info import *

# read in the file that tracks the most recent breach for each state
recent = pd.read_csv('recent.csv').set_index('state')

# initiate an empty data frame to capture the new breaches 
columns = ['Name of Entity', 'Dates of Breach', 'Reported Date',
       'Date(s) of Discovery of Breach', 'Date Notice Provided to Consumers',
       'Data Stolen', 'Type of Breach','Individuals Affected',
       'State Reported','Link to PDF','Location of Breached Information']
new_this_run = pd.DataFrame(columns = columns)

#California
ca_df, ca_recent = update_California(recent['recent']['California'])
recent['recent']['California'] = ca_recent

if os.path.isfile('california.csv'):
	california = pd.read_csv('california.csv')
	ca_df.append(california,ignore_index=True).to_csv('california.csv', index = False)
else:
	ca_df.to_csv('california.csv', index = False)
new_this_run = new_this_run.append(ca_df,ignore_index=True)
print ('\n', 'Fetched California Data', '\n')

#Delaware
if os.path.isfile('delaware.csv'):
	delaware = pd.read_csv('delaware.csv')
	de_df = update_Delaware(delaware)
	de_df[0].append(delaware,ignore_index=True).to_csv('delaware.csv', index = False)
else:
	delaware = pd.DataFrame(columns = ['Link to PDF'])
	de_df = update_Delaware(delaware)
	de_df[0].to_csv('delaware.csv', index = False)

new_this_run = new_this_run.append(de_df[0],ignore_index=True)
print ('\n', 'Fetched Delaware Data', '\n')

#Indiana
try:
	recency = pd.read_csv('indiana_recency.csv')
except:
	recency = pd.DataFrame(columns = ['date,name'])
in_df = update_Indiana(recency)
#recent['recent']['Indiana'] = in_recent

if os.path.isfile('indiana.csv'):
	indiana = pd.read_csv('indiana.csv')
	indiana.append(in_df,ignore_index=True).to_csv('indiana.csv', index = False)
else:

	in_df.to_csv('indiana.csv', index = False)
new_this_run = new_this_run.append(in_df,ignore_index=True)
print ('\n', 'Fetched Indiana Data', '\n')

#Iowa
ia_df, ia_recent, ia_other = update_Iowa(recent['recent']['Iowa'],recent['other']['Iowa'])
recent['recent']['Iowa'] = ia_recent
recent['other']['Iowa'] = ia_other

if os.path.isfile('iowa.csv'):
	iowa = pd.read_csv('iowa.csv')
	iowa.append(ia_df,ignore_index=True).to_csv('iowa.csv', index = False)
else:
	ia_df.to_csv('iowa.csv', index = False)
new_this_run = new_this_run.append(ia_df,ignore_index=True)
print ('\n', 'Fetched Iowa Data', '\n')

#Maine
me_df, me_recent = update_Maine(recent['recent']['Maine'])
recent['recent']['Maine'] = me_recent

if os.path.isfile('maine.csv'):
	maine = pd.read_csv('maine.csv')
	me_df.append(maine,ignore_index=True).to_csv('maine.csv', index = False)
else:
	me_df.to_csv('maine.csv', index = False)
new_this_run = new_this_run.append(me_df,ignore_index=True)
print ('\n', 'Fetched Maine Data', '\n')

#Maryland
md_df, md_recent = update_Maryland(recent['recent']['Maryland'])
recent['recent']['Maryland'] = md_recent
if os.path.isfile('maryland.csv'):
	maryland = pd.read_csv('maryland.csv')
	md_df.append(maryland,ignore_index=True).to_csv('maryland.csv', index = False)
else:
	md_df.to_csv('maryland.csv', index = False)

new_this_run = new_this_run.append(md_df, ignore_index = True)
print ('\n', 'Fetched Maryland Data', '\n')

#Montana
mo_df, mo_recent = update_Montana(recent['recent']['Montana'])
recent['recent']['Montana'] = mo_recent

if os.path.isfile('montana.csv'):
	montana = pd.read_csv('montana.csv')
	mo_df.append(montana,ignore_index=True).to_csv('montana.csv', index = False)
else:
	mo_df.to_csv('montana.csv', index = False)

new_this_run = new_this_run.append(mo_df,ignore_index=True)
print ('\n', 'Fetched Montana Data', '\n')

#New Hampshire
if os.path.isfile('newhampshire.csv'):
	newhampshire = pd.read_csv('newhampshire.csv')
	nh_df = update_NewHampshire(newhampshire)
	nh_df.append(newhampshire,ignore_index=True).to_csv('newhampshire.csv', index = False)
else:
	newhampshire = pd.DataFrame(columns = ['Link to PDF'])
	nh_df = update_NewHampshire(newhampshire)
	nh_df.to_csv('newhampshire.csv', index = False)

new_this_run = new_this_run.append(nh_df,ignore_index=True)
print ('\n', 'Fetched New Hampshire Data', '\n')

#New Jersey
nj_df, nj_recent = update_NewJersey(recent['recent']['New Jersey'])
recent['recent']['New Jersey'] = nj_recent
if os.path.isfile('newjersey.csv'):
	newjersey = pd.read_csv('newjersey.csv')
	nj_df.append(newjersey,ignore_index=True).to_csv('newjersey.csv', index = False)
else:
	nj_df.to_csv('newjersey.csv', index = False)

new_this_run = new_this_run.append(nj_df,ignore_index=True)
print ('\n', 'Fetched New Jersey Data', '\n')

#Oregon
or_df, or_recent = update_Oregon(recent['recent']['Oregon'])
recent['recent']['Oregon'] = or_recent

if os.path.isfile('oregon.csv'):
	oregon = pd.read_csv('oregon.csv')
	or_df.append(oregon,ignore_index=True).to_csv('oregon.csv', index = False)
else:
	or_df.to_csv('oregon.csv', index = False)

new_this_run = new_this_run.append(or_df,ignore_index=True)
print ('\n', 'Fetched Oregon Data', '\n')

#US Department of Health
usdh_df, usdh_recent = update_USDeptHealth(recent['recent']['Department of Health'])
recent['recent']['Department of Health'] = usdh_recent

if os.path.isfile('usdh.csv'):
	usdh = pd.read_csv('usdh.csv')
	usdh_df.append(usdh,ignore_index=True).to_csv('usdh.csv', index = False)
else:
	usdh_df.to_csv('usdh.csv', index = False)
new_this_run = new_this_run.append(usdh_df,ignore_index=True)
print ('\n', 'Fetched USDH Data', '\n')

#Vermont
vt_df, vt_recent, vt_other = update_Vermont(recent['recent']['Vermont'],recent['other']['Vermont'])
recent['recent']['Vermont'] = vt_recent
recent['other']['Vermont'] = vt_other

if os.path.isfile('vermont.csv'):
	vermont = pd.read_csv('vermont.csv')
	vt_df.append(vermont,ignore_index=True).to_csv('vermont.csv', index = False)
else:
	vt_df.to_csv('vermont.csv', index = False)

new_this_run = new_this_run.append(vt_df,ignore_index=True)
print ('\n', 'Fetched Vermont Data', '\n')

#Washington
wa_df, wa_recent = update_Washington(recent['recent']['Washington'])
recent['recent']['Washington'] = wa_recent
if os.path.isfile('washington.csv'):
	washington = pd.read_csv('washington.csv')
	wa_df.append(washington,ignore_index=True).to_csv('washington.csv', index = False)
else:
	wa_df.to_csv('washington.csv', index = False)

new_this_run = new_this_run.append(wa_df, ignore_index = True)
print ('\n', 'Fetched Washington Data', '\n')

#Wisconsin
wi_df, wi_recent = update_Wisconsin(recent['recent']['Wisconsin'])
recent['recent']['Wisconsin'] = wi_recent

if os.path.isfile('wisconsin.csv'):
	wisconsin = pd.read_csv('wisconsin.csv')
	wi_df.append(wisconsin,ignore_index=True).to_csv('wisconsin.csv', index = False)
else:

	wi_df.to_csv('wisconsin.csv', index = False)

new_this_run = new_this_run.append(wi_df, ignore_index = True)
print ('\n', 'Fetched Wisconsin Data', '\n')

# Clean new data
new_this_run.to_csv('test_new.csv')
cleaned_new_df = new_this_run
added_data = add_data(new_this_run)
cleaned_new_df = clean_new_data(added_data)
type1_list = get_breach_type_cause(cleaned_new_df)
type2_list = get_breach_type_classifier(cleaned_new_df)
cleaned_new_df['Type of Breach'] = final_list(type1_list, type2_list)
cleaned_new_df = cleaned_new_df.drop(columns = ['PDF text'])

# Combine new data with existing data and clean
if os.path.isfile('data_breach_chronology_new.csv'):
	data_breach_chronology = pd.read_csv('data_breach_chronology_new.csv')
	all_data = cleaned_new_df.append(data_breach_chronology)
else:
	all_data = cleaned_new_df
final_df = clean_ents(all_data)
final_df = remove_duplicates(final_df)
#final_df = final_df.drop(columns = ['Entity Type', 'Details', 'Link'])

# export data breach chronology and update the recent file
final_df.to_csv('data_breach_chronology_new.csv', index = False)
recent.to_csv('recent.csv')
