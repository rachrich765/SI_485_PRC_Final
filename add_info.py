import wikipedia
import pandas as pd
import re

# Wikipedia API
def wikipedia_api(company_name):
	# Check if there is a wikipedia page for the entity 
	try:
		x = wikipedia.page(company_name)
		url = x.url
		# Gather the info box
		info_box = pd.read_html(url, attrs = {'class':'infobox'})[0].set_index(0)
	# Handle the case where there is no wikipedia page
	except:
		info_box = ''
	print ('wikipedia')
	return info_box

#Gather industry from info box if there
def get_industry(info_box):
	try:
		industry = info_box.loc['Industry',1]
	except:
		industry = ''
	return industry

#Gather website from info box if there
def get_website(info_box):
	try:
		website  = re.findall(r'w*\.*[a-zA-Z0-9]+\.com',info_box.loc['Website',1])[0]
	except:
		website = ''
	return website

#Gather parent company from info box if there
def get_parent(info_box):
	try:
		parent = info_box.loc['Parent',1]
	except:
		parent = ''
	return parent

#Add all gathered information to the data breach chronology dataframe
def add_data(data_breach_chronology):
	data_breach_chronology['temp'] = data_breach_chronology['Name of Entity'].apply(wikipedia_api)
	data_breach_chronology['Industry'] = data_breach_chronology['temp'].apply(get_industry)
	data_breach_chronology['Website'] = data_breach_chronology['temp'].apply(get_website)
	data_breach_chronology['Parent Company'] = data_breach_chronology['temp'].apply(get_parent)
	data_breach_chronology = data_breach_chronology.drop(columns=['temp'])
	return data_breach_chronology
