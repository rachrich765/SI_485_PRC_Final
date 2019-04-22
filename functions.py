from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from pandas.io.json import json_normalize
from urllib.parse import urlparse
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime
from tabula import read_pdf
from pathlib import Path
import lxml.html as lh
import pandas as pd
import numpy as np
import textract
import requests
import datetime
import urllib
import json
import time
import csv
import os
import re

# Takes a url and returns html contents of the page
def basic_beautiful_soup(url):
    
    code = requests.get(url)
    plain = code.text
    s = BeautifulSoup(plain, "html.parser")
    return s
    
# Takes a link to a pdf and returns the text contents if possible
def download_parse_file(url):
    
    # Get the contents of the pdf file
    try:

        response = requests.get(url)
        my_raw_data = response.content
        # Write the contents into a temporary file
        with open("new_pdf.pdf", 'wb') as my_data:
            my_data.write(my_raw_data)
        my_data.close()
    
        # Try to convert the contents of the pdf to a string     
        try:
            file = textract.process('new_pdf.pdf', method='pdfminer')
        except:
            file = "Unreadable File"   

        # Delete the temporary file
        os.remove("new_pdf.pdf")
    except: 
        return 
        'no PDF'

    # Return the string contents of the pdf
    return file

# Takes date in format mm/dd/yy or mm-dd-yy and returns date in datetime format
def guess_date_dashesandslashes(date):
    date = str(date)
    if date.count('-') > 1:
        split_date = str(date).split("-")
        if(len(split_date) == 2):
            date = "-".join([split_date[0],"01",split_date[1]])
        try:
            date = datetime.datetime.strptime(date, "%m-%d-%y")
            return date
        except:
            return date

    elif date.count('/') > 1:

        split_date = str(date).split("/")
        if(len(split_date) == 2):
            date = "/".join([split_date[0],"01",split_date[1]])
        try:
            date = datetime.datetime.strptime(date, "%m/%d/%y")
            return date
        except:
            return date
    else:
        return date

# Takes date in format mm/dd/yyyy and returns date in datetime format
def guess_date_mm_dd_yyyy(date):
    split_date = str(date).split("/")
    if(len(split_date) == 2):
        date = "/".join([split_date[0],"01",split_date[1]])
    #print (date)
    try:
        date = datetime.datetime.strptime(date, "%m/%d/%Y")
        return date

    except:
        return date

# Takes date in format yyyy-mm-dd and returns date in datetime format
def guess_date_yyyy_mm_dd(date):
    split_date = str(date).split("-")
    if(len(split_date) == 2):
        date = "-".join([split_date[0],"01",split_date[1]])
    #print (date)
    try:
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        return date

    except:
        return date

# Takes date in format mm/dd/yy and returns date in datetime format
def guess_date_mm_dd_yy(date):
    split_date = str(date).split("/")
    if(len(split_date) == 2):
        date = "/".join([split_date[0],"01",split_date[1]])
    #print (date)
    try:
        date = datetime.datetime.strptime(date, "%m/%d/%y")
        return date

    except:
        return date

# Takes date in format day, mm dd, yyyy and returns date in datetime format
def date_w_day(date):
    try:
        date = datetime.datetime.strptime(date, "%A, %B %d, %Y")
        return date
    except:
        return date

# Takes date in format mm dd, yyyy and returns date in datetime format
def guess_date_month_dd_yyyy(date):
    try:
        date = datetime.datetime.strptime(date, "%B %d, %Y")
        return date
    except:
        return date

def update_Oregon(most_recent_breach):

    more_info_urls = []
    main_url = 'https://justice.oregon.gov/consumer/DataBreach/Home/'
    oregon = pd.read_html(main_url)[0]
    oregon.columns = ['Organization','Dates of Breach', 'Reported Date']
    s = basic_beautiful_soup(main_url)
    
    for x in s('table', {'class':"webgrid-table", 'id':"grid"}):
        for item in x('a'):
            more_info_urls.append('https://justice.oregon.gov' + item.get('href'))

    breaches_info = {'Organization Name': [], 'Reported Date': [],
                    'Dates of Breach':[], 'Date(s) of Discovery of Breach': [],
                    'Notice Provided to Consumers':[],'Link':[]}
    
    
    for url in more_info_urls:
        #if url != most_recent_breach:
        try:
            temp_dict = {}
            s2 = basic_beautiful_soup(url)
            l = []
            for x in s2('td'):
                l.append(x.text.strip(':').strip('\r').strip('\n'))
            for item in l[::2]:
                temp_dict[item] = l[l.index(item)+1]
            breaches_info['Organization Name'].append(temp_dict.get('Organization Name', ''))  
            breaches_info['Reported Date'].append(temp_dict.get('Reported Date', ''))
            breaches_info['Dates of Breach'].append(temp_dict.get('Date(s) of Breach', ''))
            breaches_info['Date(s) of Discovery of Breach'].append(temp_dict.get('Date(s) of Discovery of Breach', ''))
            breaches_info['Notice Provided to Consumers'].append(temp_dict.get('Notice Provided to Consumers', ''))
            breaches_info['Link'].append(url)
        except:
            pass
        #else:
        #    break 
    more_info = pd.DataFrame(breaches_info)

    more_info = more_info.drop(columns = ['Reported Date', 'Dates of Breach'])
    more_info = more_info.rename(columns={"Organization Name": "Organization"})

    #more_info.columns = ['Date(s) of Discovery of Breach', 'Notice Provided to Consumers' ,'Organization']
    
    df = pd.merge(oregon, more_info, how = 'left', on = 'Organization')
    df = df.reset_index(drop=True)
    df = df.rename(index=str, columns={"Organization": "Name of Entity", 'Notice Provided to Consumers': 'Date Notice Provided to Consumers'})
    df['State Reported'] = 'Oregon'
    df['Source(s)'] = 'https://justice.oregon.gov/consumer/DataBreach/Home/'

    split_date = df['Dates of Breach'].apply(lambda x: re.split(' - |,', str(x)))
    df["Start Date of Breach"] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[0].strip('Between ')))
    df['End Date of Breach'] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[-1]) if len(x) > 1 else None)
    df['Reported Date'] = df['Reported Date'].apply(guess_date_mm_dd_yyyy)
    df['Date(s) of Discovery of Breach'] = df['Date(s) of Discovery of Breach'].apply(date_w_day)
    df['Date Notice Provided to Consumers'] = df['Date Notice Provided to Consumers'].apply(date_w_day)
    df = df.reset_index(drop=True)
    #print (most_recent_breach in df['Link'])
    oregon = df[int(df.index[df['Name of Entity'] == most_recent_breach][0]):]
    try:
        oregon = df[int(df.index[df['Name of Entity'] == most_recent_breach][0]):]
    except:
        oregon = df

    most_recent_breach = oregon['Name of Entity'].iloc[-1]
    oregon = oregon.drop(columns = ['Link'])
    return oregon, most_recent_breach


def update_Wisconsin(most_recent_breach):

    current_url = 'https://datcp.wi.gov/Pages/Programs_Services/DataBreaches.aspx'
    temp = pd.read_html(current_url)
    rows_list = []
    for breach in temp:
        row = {'Date Notice Provided to Consumers':breach.iloc[1][0],
               'Dates of Breach':breach.iloc[1][1],
               'Name of Entity':breach.iloc[1][2],
               'Data Stolen':breach.iloc[1][3],
               "Individuals Affected":breach.iloc[3][0],
               'Details':breach.iloc[3][1]}
        rows_list.append(row)
    wisconsin_now = pd.DataFrame(rows_list)

    pre_current_year_url = 'https://datcp.wi.gov/Pages/Programs_Services/DataBreachArchive.aspx'
    temp = pd.read_html(pre_current_year_url)
    rows_list = []
    for breach in temp:
        row = {'Date Notice Provided to Consumers':breach.iloc[1][0],
               'Dates of Breach':breach.iloc[1][1],
               'Name of Entity':breach.iloc[1][2],
               'Data Stolen':breach.iloc[1][3], 
               "Individuals Affected":breach.iloc[3][0],
               'Details':breach.iloc[3][1]}
        rows_list.append(row)
    wisconsin_archive = pd.DataFrame(rows_list)

    wisconsin = pd.concat([wisconsin_now, wisconsin_archive], ignore_index = True)
    wisconsin = wisconsin.drop(columns = ['Details'])
    wisconsin['State Reported'] = 'Wisconsin'
    wisconsin['Source(s)'] = 'https://datcp.wi.gov/Pages/Programs_Services/DataBreaches.aspx'
    split_date = wisconsin['Dates of Breach'].apply(lambda x: re.split('-|–| to |and| through ', str(x)))
    #split_date = df['Dates of Breach'].apply(lambda x: re.split('-', str(x)))
    wisconsin["Start Date of Breach"] = split_date.apply(lambda x: guess_date_month_dd_yyyy(x[0].strip('Between ')))
    wisconsin['End Date of Breach'] = split_date.apply(lambda x: guess_date_month_dd_yyyy(x[1]) if len(x) > 1 else None)
    wisconsin['Date Notice Provided to Consumers'] = wisconsin['Date Notice Provided to Consumers'].apply(guess_date_month_dd_yyyy)

    try:
        wisconsin = wisconsin[:wisconsin.index[wisconsin['Name of Entity'] == most_recent_breach][0]+1]
    except:
        pass

    most_recent_breach = wisconsin['Name of Entity'].iloc[0]
    return wisconsin, most_recent_breach


def update_Vermont(most_recent_breach, finished_year):
    url = 'https://ago.vermont.gov/archived-security-breaches/'
    base_url = 'https://ago.vermont.gov'

    s = basic_beautiful_soup(url)

    year_urls= []
    recent = {}
    
    for x in s('li', {'class':"awDatesLI"}):
        for item in x('a'):
            year_urls.append(item.get('href'))

    vermont_dict = {'Name of Entity':[],
                    'Link to PDF':[]}
    for year in year_urls:
        if year != finished_year:
            breaches = []
            s_year = basic_beautiful_soup(year)

            for x in s_year('div', {'id':"postWrapper"}):
                for item in x('a'):
                    breaches.append((item('h3',{'class':'awyca_subheader'})[0].text,item.get('href')))
            recent[year] = breaches[0][1]
            for breach in breaches:
                #if breach[1] != most_recent_breach:
                s_breach = basic_beautiful_soup(breach[1])
                pdf_extension = s_breach('a',{'class':"pdfemb-viewer"})[0].get('href')
                total_url = base_url + pdf_extension
                    #file = download_parse_file(total_url)
                vermont_dict['Name of Entity'].append(breach[0].replace('Notice of Data Breach to Consumers','').replace('SBN to Consumers',''))
                    #vermont_dict['PDF text (ALL)'].append(file)
                vermont_dict['Link to PDF'].append(breach[1])
                #else:
                #    break
        else:
            break

    #print (len(vermont['Name of Entity']))
    #print (len(vermont['Name of Entity']))
    #print (len(vermont['Name of Entity']))

    vermont = pd.DataFrame(vermont_dict)
    vermont['State Reported'] = 'Vermont'
    vermont['Source(s)'] = 'https://ago.vermont.gov/archived-security-breaches/'
    vermont['Date Notice Provided to Consumers'] = vermont['Link to PDF'].apply(lambda x: x[34:36]+'/'+x[37:39]+'/'+x[29:33]) 
    vermont['Date Notice Provided to Consumers'] = vermont['Date Notice Provided to Consumers'].apply(guess_date_mm_dd_yyyy)

    try:
        vermont = vermont[:int(vermont.index[vermont['Link to PDF'] == most_recent_breach][0])+1]
    except:
        pass

    most_recent_breach = vermont['Link to PDF'][0]

    finished_year = year_urls[1]
    most_recent_breach = recent[year_urls[0]]
    

    return vermont, most_recent_breach ,finished_year
    

def update_Washington(most_recent_breach):
    url = 'http://www.atg.wa.gov/data-breach-notifications'

    washington = pd.read_html(url, skiprows = 2)[1]
    
    washington = washington.rename(index = str, columns = {0:'Reported Date', 1:'Name of Entity', 2:'Dates of Breach'})
    washington = washington.dropna()
    washington['State Reported'] = 'Washington'
    washington['Source(s)'] = 'https://www.atg.wa.gov/data-breach-notifications'

    s = basic_beautiful_soup(url)
    pdf_links = []
    for breach in s('td')[5:]:
        if breach('a'):
            pdf_links.append(breach('a')[0].get('href'))

    washington['Link to PDF'] = pdf_links

    split_date = washington['Dates of Breach'].apply(lambda x: re.split(' to ', str(x)))
    washington['Reported Date'] = washington['Reported Date'].apply(guess_date_mm_dd_yyyy)    #split_date = df['Dates of Breach'].apply(lambda x: re.split('', str(x)))
    washington["Start Date of Breach"] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[0]))
    washington['End Date of Breach'] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[-1]) if len(x) > 1 else None)
    
    try:
        index = int(washington.index[washington['Link to PDF'] == most_recent_breach][0])
        washington = washington[:index+1]
    except:
        pass

    most_recent_breach = pdf_links[0]
    #oregon = oregon.drop(columns = ['Link'])
    return washington, most_recent_breach

    #washington['PDF text (ALL)'] = washington['Link to PDF'].apply(download_parse_file)

    #print (washington.head())


def update_California(most_recent_breach):

    url = 'https://oag.ca.gov/privacy/databreach/list'
    
    s = basic_beautiful_soup(url)
    #breach_table = s.find('table')
    #headers = [header.text for header in breach_table.find_all('th')]
    
    #rows = []
    #for row in breach_table.find_all('tr'):
    #    rows.append([val.text.encode('utf8') for val in row.find_all('td')])

    #california = pd.DataFrame(columns = headers, data =rows[1:])
    california = pd.read_html(url)[0]
    california = california.rename(index=str, columns={"Organization Name": "Name of Entity", 'Date(s) of Breach': 'Dates of Breach'})

    links = s.tbody.find_all('a')
    pdfs = []
    for link in links:
        pdfs.append(link.get('href'))

    california['Link to PDF'] = pdfs

    split_date = california['Dates of Breach'].apply(lambda x: re.split(', ', str(x)))
    california["Start Date of Breach"] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[0]))
    california['End Date of Breach'] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[-1]) if len(x) > 1 else None)
    california['Reported Date'] = california['Reported Date'].apply(guess_date_mm_dd_yyyy)
    #california['Date(s) of Discovery of Breach'] = california['Date(s) of Discovery of Breach'].apply(guess_date_mm_dd_yyyy)

    try:
        california = california[:int(california.index[california['Link to PDF'] == most_recent_breach][0])+1]
    except:
        pass

    california['State Reported'] = 'California'
    california['Source(s)'] ='https://oag.ca.gov/privacy/databreach/list'
    most_recent_breach = pdfs[0]

    return california, most_recent_breach

def update_Indiana(most_recent_breaches):

    drop_these = most_recent_breaches['date,name']
    
    url = 'https://www.in.gov/attorneygeneral/2874.htm'
    url_front = 'https://www.in.gov/'
    #columns = ['Name of Entity','Date Notice Provided to Consumers','Dates of Breach','Number of IN Residents Affected','Number of Consumers Affected Nationwide','Disposition','Status']
    columns = ['Name of Entity', 'Date Notice Provided to Consumers',
       'Dates of Breach', 'Number of IN Residents Affected',
       'Number of Consumers Affected Nationwide', 'Status']
    indiana = pd.DataFrame(columns=columns)

    s = basic_beautiful_soup(url)
    breach_links = s.find('p', relativehref='[ioID]8026AD70E4164E4E9F9FE8FA5553BA5C/2016_04_01_ITU_Breach(1).pdf')('a')
    full_breach_links = []
    for link in breach_links:
        full_breach_links.append(url_front + link.get('href'))
    for url in full_breach_links[2:]:
        if '.pdf' in url:
            pass
        #    response = requests.get(url)
        #    my_raw_data = response.content
        #    with open("new_pdf.pdf", 'wb') as my_data:
        #        my_data.write(my_raw_data)
        #        my_data.close()
        #    if url == 'https://www.in.gov//attorneygeneral/files/2014_Security_Breach_Information_November_2015_2014.pdf':
        #        temp = read_pdf("new_pdf.pdf", lattice = True) 
        #        temp.columns = [ 'Name of Entity', 'Date Notice Provided to Consumers','Dates of Breach', 'Number of IN Residents Affected','Disposition']
        #        temp = temp.drop(columns = [ 'Disposition'])
        #        temp['Name of Entity'] = temp['Name of Entity']#.str.strip('\t\r').str.strip('\n')
        #    else:
        #        temp = read_pdf("new_pdf.pdf", pandas_options = {'header': None})
        #        temp.columns = [ 'Name of Entity', 'Date Notice Provided to Consumers','Dates of Breach', 'Number of IN Residents Affected','Number of Consumers Affected Nationwide','Disposition']
        #        temp = temp.drop(columns = [ 'Disposition'])
        #    print (temp.columns)
        #    indiana = indiana.append(temp, ignore_index = True)
        #    os.remove("new_pdf.pdf")
        else:
            resp = requests.get(url)
            output = open('in_one_year.xls', 'wb')
            output.write(resp.content)
            output.close()

            temp = pd.read_excel('in_one_year.xls') 
            os.remove('in_one_year.xls')
            if 'Respondent' in temp.columns:
                #print('x')
                temp_new = temp.rename(index=str, columns={'Respondent':'Name of Entity',  
                    'Notification Sent':'Date Notice Provided to Consumers',
                    'Breach Occurred':'Dates of Breach',
                    'IN Affected':'Number of IN Residents Affected',
                    'Total Affected':'Number of Consumers Affected Nationwide'})
                
                #indiana = indiana.append(temp_new, ignore_index = True)
                #print (temp.columns)
            elif "Name of Company or Organization " in temp.columns:
                #print('y')
                temp_new = temp.rename(index=str, columns={'Name of Company or Organization ':'Name of Entity', 
                    'Date of Notification ':'Date Notice Provided to Consumers', 
                    'Date of Breach ':'Dates of Breach',
                    'Number of Indiana Residents Affected ':'Number of IN Residents Affected',
                    'Number of Consumers Affected Nationwide ':'Number of Consumers Affected Nationwide',
                    'Disposition':'Status'})
            else:
                #print('y')
                temp_new = temp.rename(index=str, columns={'Name of Company or Organization':'Name of Entity', 
                    'Date of Notification ':'Date Notice Provided to Consumers', 
                    'Date of Breach ':'Dates of Breach',
                    'Number of Indiana Residents Affected':'Number of IN Residents Affected',
                    'Number of Consumers Affected Nationwide ':'Number of Consumers Affected Nationwide',
                    'Disposition':'Status'})
               
            indiana = indiana.append(temp_new, ignore_index = True)#in temp.columns:
    
    indiana['Number of Consumers Affected Nationwide'] = indiana['Number of Consumers Affected Nationwide'].apply(lambda x: 'Unknown' if pd.isnull(x) else str(x))
    indiana['Number of IN Residents Affected'] = indiana['Number of IN Residents Affected'].apply(lambda x: 'Unknown' if pd.isnull(x) else str(x))
    indiana['Individuals Affected'] = indiana['Number of Consumers Affected Nationwide']+' ('+ indiana['Number of IN Residents Affected'] +' in Indiana)'
    indiana = indiana.drop(columns = ['Number of Consumers Affected Nationwide','Number of IN Residents Affected','Status', 'Unnamed: 1'])
    
    indiana['State Reported'] = 'Indiana'
    indiana['Source(s)'] = 'https://www.in.gov/attorneygeneral/2874.htm'
    indiana['date,name'] = indiana['Name of Entity'] + '|' + str(indiana['Dates of Breach'].fillna('none'))
    indiana.to_csv('indiana_recency.csv')
    indiana['Date Notice Provided to Consumers'] = indiana['Date Notice Provided to Consumers'].apply(guess_date_yyyy_mm_dd)   #split_date = df['Dates of Breach'].apply(lambda x: re.split('', str(x)))
    indiana['Start Date of Breach'] = indiana['Dates of Breach'].apply(guess_date_yyyy_mm_dd)
    def clean_names(name):
        split = re.findall('[A-Z]*[a-z]*', name)
        new_name = ' '.join(split)
        return new_name
    indiana['Name of Entity'] = indiana['Name of Entity'].apply(clean_names)
  
    #print (indiana.index[indiana['Name of Entity']== most_recent_breach])

    indiana = indiana[~indiana['date,name'].isin(drop_these)]
    indiana = indiana.drop(columns = ['date,name'])

    return indiana #new_most_recent_breach

def update_Iowa(most_recent_breach, finished_year):
    finished_year = 0
    new_most_recent_breach = ''
    base_url = 'https://www.iowaattorneygeneral.gov'
    first_year = 2011
    last_year = datetime.datetime.today().year
    columns = ['Name of Entity', 'Reported Date']
    iowa = pd.DataFrame(columns=columns)
    years = range(first_year,last_year+1)
    for year in years:
        if year > float(finished_year):
            if (year != 2017 and year != 2019):
                url = 'https://www.iowaattorneygeneral.gov/for-consumers/security-breach-notifications/{}-security-breach-notifications/'.format(year)
            else:
                url = 'https://www.iowaattorneygeneral.gov/for-consumers/security-breach-notifications/{}'.format(year)
            temp = pd.read_html(url)[0]
            temp = temp.rename(columns=temp.iloc[0])
            temp = temp.drop(temp.index[0])
            temp = temp.dropna(how = 'all')
            s = basic_beautiful_soup(url)
            links = s.find('table')('a')
            pdfs = []
            links_pdfs = []
            for item in links:
                link = item.get('href')
                if link != most_recent_breach:
                    url = base_url + item.get('href')
                    links_pdfs.append(url)
                    #pdfs.append(download_parse_file(url))
            
            #pdfs_df = pd.DataFrame({'Link to PDF':links_pdfs})
            temp['Link to PDF'] = links_pdfs
            #try:
            #    new_most_recent_breach = links_pdfs[-1]
            #except:
            #    new_most_recent_breach = most_recent_breach
            #temp = temp.dropna(subset=['Link to PDF'])

                #if item.get('href'):
                    #url = base_url + item.get('href')
                    #links_pdfs.append(url)
                    #pdfs.append(download_parse_file(url))
                #else:
                #    pdfs.append('')
 
            #breach_table = s.find('table')
            #headers = [header.text for header in breach_table.find_all('h4')]
     
            #temp['PDF text (ALL)'] = pdfs
            #temp['Link to PDF'] = links_pdfs
            

            #print (temp.columns)
            if 'YEAR REPORTED' in temp.columns:
                temp['YEAR REPORTED'] = year
                temp = temp.rename(columns = {'YEAR REPORTED':'Reported Date','ORGANIZATION NAME':'Name of Entity'})
            else: 
                temp = temp.rename(columns = {'DATE REPORTED':'Reported Date','ORGANIZATION NAME':'Name of Entity'})
            iowa = iowa.append(temp,ignore_index=True)
    iowa['State Reported'] = 'Iowa'
    iowa['Source(s)'] = 'https://www.iowaattorneygeneral.gov/for-consumers/security-breach-notifications/'
    iowa['Reported Date'] = iowa['Reported Date'].apply(guess_date_dashesandslashes)
    new_most_recent_breach = iowa['Link to PDF'].iloc[-1]
    try:
        iowa = iowa[iowa.index[iowa['Link to PDF'] == most_recent_breach][0]+1:]
    except:
        pass

    finished_year = years[-2]

    return iowa, new_most_recent_breach, finished_year

def update_Delaware(de_df):
    
    drop_these = de_df['Link to PDF']

    url = 'https://attorneygeneral.delaware.gov/fraud/cpu/securitybreachnotification/database/'
    delaware = pd.read_html(url)[0]
    s = basic_beautiful_soup(url)

    links = s.tbody.find_all('a')

    #i = 0
    pdfs = []
    for link in links:
        try:
            #print (link.get('href'))
            #pdfs.append(download_parse_file(link.get('href')))
            pdfs.append(link.get('href'))
            #i+=1
    
        except:
            #i+=1
            pass
    
    delaware = delaware.rename(index=str, columns={'Organization Name': 'Name of Entity', 'Date(s) of Breach':'Dates of Breach',
        'Number of Potentially Affected Delaware Residents': 'Individuals Affected'})
    delaware = delaware.drop(columns = ["Sample of Notice"])
    
    delaware['Individuals Affected'] = delaware['Individuals Affected'].apply(lambda x: str(x)+'(in Delaware)')
    delaware['Link to PDF'] = pdfs
    delaware['State Reported'] = 'Delaware'
    delaware['Source(s)'] = 'https://attorneygeneral.delaware.gov/fraud/cpu/securitybreachnotification/database/'

    split_date = delaware['Dates of Breach'].apply(lambda x: re.split('–', str(x)))
    delaware["Start Date of Breach"] = split_date.apply(lambda x: guess_date_mm_dd_yy(x[0].strip()))
    delaware['End Date of Breach'] = split_date.apply(lambda x: guess_date_mm_dd_yy(x[1].strip()) if len(x) > 1 else None)
    delaware['Reported Date'] = delaware['Reported Date'].apply(guess_date_mm_dd_yy)
    #df['Date(s) of Discovery of Breach'] = df['Date(s) of Discovery of Breach'].apply(guess_date)

    delaware = delaware[~delaware['Link to PDF'].isin(drop_these)]
    
    return delaware, datetime.datetime.now()


def update_NewHampshire(nh_df):

    new_NH_dates = list()
    new_NH_dates2 = list()
    new_NH_dates3 = list()
    new_NH_dates4 = list()
    new_NH_dates5 = list()
    def clean_NH_data(NH_data):
        NH_data = NH_data
        for x in NH_data['Dates of Breach']:
            x = str(x)
            try: 
                d = datetime.datetime.strptime(x, "%d-%b-%y")
                new_NH_dates.append(d)
            except: 
                new_NH_dates.append(x)
    
        NH_data['Start Date of Breach'] = new_NH_dates
    
        for x in NH_data['Start Date of Breach']:
            try: 
                d = datetime.datetime.strptime(x, "%B %d, %Y")
                new_NH_dates2.append(d)
            except: 
                new_NH_dates2.append(x)

        NH_data['Start Date of Breach'] = new_NH_dates2

        for x in NH_data['Start Date of Breach']:
            try: 
                d = datetime.datetime.strptime(x, "%B %d. %Y")
                new_NH_dates3.append(d)
            except: 
                new_NH_dates3.append(x)

        NH_data['Start Date of Breach'] = new_NH_dates3
        for x in NH_data['Start Date of Breach']:
            try: 
                d = datetime.datetime.strptime(x, "%B %d %Y")
                new_NH_dates4.append(d)
            except: 
                new_NH_dates4.append(x)

        NH_data['Start Date of Breach'] = new_NH_dates4
        for x in NH_data['Start Date of Breach']:
            try: 
                d = datetime.datetime.strptime(x, "%B %d,%Y")
                new_NH_dates5.append(d)
            except: 
                new_NH_dates5.append(x)
        NH_data['Start Date of Breach'] = new_NH_dates5
        NH_data['End Date of Breach'] = ''
        return NH_data
    
    drop_these = nh_df['Link to PDF']

    url = 'https://www.doj.nh.gov/consumer/security-breaches/'
    s = basic_beautiful_soup(url)
    
    all_p = (s.find_all('p'))
    letter_links = list()
    for p in all_p:
        letter_links_a = (p.find_all('a', href=True))
        if letter_links_a:
            #only get links for A-Z
            #for x in letter_links_a[:-1]:
            for x in letter_links_a:
                letter_links.append(x['href'])
    columns = ['company_date', 'Link to PDF']
    newhampshire = pd.DataFrame(columns = columns)
    NH_dict = dict()
    links = list()
    for x in letter_links:
        url2 = url + x
        s2 = basic_beautiful_soup(url2) 
        #titles = 
        a = s2.find_all('a', href=True)
        #print (a)
        for a2 in a:
            link = str(a2['href'])
            if link.endswith('pdf'):
                company_date = a2.text
                newhampshire = newhampshire.append({'company_date':company_date,'Link to PDF':'https://www.doj.nh.gov/consumer/security-breaches/' + link}, ignore_index=True)
                #links[company_date] = 'https://www.doj.nh.gov/consumer/security-breaches/' + link

    def get_date(row):
        pattern = r'.*?([A-Za-z]{3,10}\s*[0-9]{1,2}[,/.]*\s*[0-9]{4})'
        try:
            match = re.findall(pattern,row)[0]
            return match
        except:
            return ''
    def get_company(row):
        pattern = r'(.*?)[A-Za-z]{3,10}\s*[0-9]{1,2}[,/.]*\s*[0-9]{4}'
        try:
            match = re.findall(pattern,row)[0]
            return match
        except:
            return row

    newhampshire['Dates of Breach'] = newhampshire['company_date'].apply(get_date)
    newhampshire['Name of Entity'] = newhampshire['company_date'].apply(get_company)
    newhampshire = newhampshire.drop(['company_date'], axis = 1)   

    #newhampshire['PDF text (ALL)'] = newhampshire['Link to PDF'].apply(download_parse_file)
    
    newhampshire['State Reported'] = 'New Hampshire'
    newhampshire['Source(s)'] = 'https://www.doj.nh.gov/consumer/security-breaches/'
    newhampshire = clean_NH_data(newhampshire)

    newhampshire = newhampshire[~newhampshire['Link to PDF'].isin(drop_these)]
    #k = list(newhampshire['Link to PDF'])
    #l = list(drop_these)
    #[i for k in i if i not in l]

    return newhampshire
       

def update_NewJersey(most_recent_breach):
    def NJ_clean_dates(NJ_data):
        new_NJ_dates = list()
        NJ_data = NJ_data
        for x in NJ_data['Dates of Breach']:
            x = str(x)
            try: 
            #d = datetime.strptime(x, "%d-%b-%y")
                d = datetime.datetime.strptime(x, "%B %d, %Y")
                new_NJ_dates.append(d)
            except: 
                new_NJ_dates.append(x)

        NJ_data['Start Date of Breach'] = new_NJ_dates
        NJ_data['End Date of Breach'] = ''
        return NJ_data


    driver = webdriver.Chrome()
    links = []
    description_links = list()
    url = 'https://www.cyber.nj.gov/data-breach-alerts'
    rows = []
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    #get first item in list of postings
    a=soup.find("section", {"class": "BlogList BlogList--posts-excerpt sqs-blog-list clear"})
    x = a.find("a", href=True)
    link = x['href']
    url2 = 'https://www.cyber.nj.gov' + str(link)
    res2 = requests.get(url2)
    soup2 = BeautifulSoup(res2.content, 'html.parser')
    driver.get(url2)
    b = soup2.find("a", {"class":"BlogItem-pagination-link BlogItem-pagination-link--next"}, href=True)
    link2 = b.get('href')

    pdf_urls = list()
    #close popup window
    time.sleep(5)
    python_button = driver.find_element_by_xpath("//a[@class='sqs-popup-overlay-close']").click()

    while True:
        try:
        #pdfs = driver.find_elements_by_xpath("//a[contains(@href, '.pdf')]")
            title = driver.find_element_by_xpath("//h1[@class='BlogItem-title']").text
            date = driver.find_element_by_css_selector('time.Blog-meta-item.Blog-meta-item--date').text
            row = {'Name of Entity': title,'Dates of Breach': date}
        #if pdfs:
        #   for pdf in pdfs:
        #       link = pdf.get_attribute("href")
        #       if link not in pdf_urls:
        #           pdf_urls.append(link)
        #           rows['Link to PDF']= link
            rows.append(row)
            python_button2 = driver.find_element_by_xpath("//a[@class='BlogItem-pagination-link BlogItem-pagination-link--next']").click()
        except:
            break
    driver.quit()
    newjersey = pd.DataFrame(rows)

    newjersey['State Reported'] = 'New Jersey'
    newjersey['Source(s)'] = 'https://www.cyber.nj.gov/data-breach-notifications/'
    newjersey = NJ_clean_dates(newjersey)
    try:
        newjersey = newjersey[:int(newjersey.index[newjersey['Name of Entity'] == most_recent_breach][0])+1]
    except:
        pass

    most_recent_breach = newjersey['Name of Entity'].iloc[0]

    return newjersey, most_recent_breach

def update_USDeptHealth(most_recent_breach):

    url = 'https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf'
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    tables1 = list()
    driver = webdriver.Chrome()
    driver.get(url)
    columns = ['Expand All', 'Name of Covered Entity', 'State', 'Covered Entity Type',
           'Individuals Affected', 'Breach Submission Date', 'Type of Breach',
           'Location of Breached Information']
    usdh = pd.DataFrame(columns = columns)

    python_button = driver.find_elements_by_xpath("//a[@class='ui-paginator-page ui-state-default ui-corner-all']")
    buttons = len(python_button)

    y = driver.page_source
    tables = pd.read_html(y, header=0)
    data_USDHHSOCR = tables[1]
    usdh = usdh.append(data_USDHHSOCR, ignore_index = True)
    for i in range(buttons):
        python_button = driver.find_elements_by_xpath("//a[@class='ui-paginator-page ui-state-default ui-corner-all']")
        x = python_button[i]
        #x = python_button[-1]
        x.click()
        time.sleep(5)
        y = driver.page_source
        tables = pd.read_html(y, header=0)
        data_USDHHSOCR = tables[1]
        usdh = usdh.append(data_USDHHSOCR, ignore_index = True)
    driver.quit()
    usdh.drop(['Expand All'], axis=1)
    usdh = usdh.rename(index=str, columns={"Name of Covered Entity": "Name of Entity", 'State': 'State Reported', 
        'Covered Entity Type': "Entity Type", 'Breach Submission Date': 'Reported Date'})
    states = { 'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'GU': 'Guam', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri', 'MP': 'Northern Mariana Islands', 'MS': 'Mississippi', 'MT': 'Montana', 'NA': 'National', 'NC': 'North Carolina', 'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VI': 'Virgin Islands', 'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming' }
    usdh['State Reported'] = usdh['State Reported'].apply(lambda x: states[x])
    usdh['Source(s)'] = 'https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf'
    usdh = usdh.drop(columns = ['Expand All'])
    usdh['Reported Date'] = usdh['Reported Date'].apply(guess_date_mm_dd_yyyy)
    try:
        usdh = usdh[:int(usdh.index[usdh['Name of Entity'] == most_recent_breach][0])+1]
    except:
        pass

    most_recent_breach = usdh['Name of Entity'].iloc[0]

    return usdh, most_recent_breach

def update_Maine(most_recent_breach):
    url = 'https://www.maine.gov/ag/consumer/identity_theft/' #URL of Main webpage
    s = basic_beautiful_soup(url)

    year_url = [] #creates URL string
    for x in s('ul', {'class':"plain"}): #iterates through the neccisary HTML to get needed href
        for item in x('a'):
#       print(item)   
            if item.get('href') != 'https://appengine.egov.com/apps/nics/Maine/AGReportingForm':
                year_url.append(item.get('href')) #retrieves and asisngs needed content (str) to year_url varialbe
    
    core_year_url = 'https://www.maine.gov/ag'
    current_breaches = core_year_url + year_url[0][5:]

    resp = requests.get(current_breaches)
    output = open('Maine.xls', 'wb')
    output.write(resp.content)
    output.close()

    maine_current = pd.read_excel('Maine.xls') 
    os.remove("Maine.xls")
    #print (maine_current.columns)
    maine_current['Entity Address'] = maine_current['01_03_02_Street Address'] + maine_current['01_03_03_City']+ maine_current['01_03_04_State'] + maine_current['01_03_05_Zip Code']
    maine_current['01_04_01_Educational'] = maine_current['01_04_01_Educational'].apply(lambda x: 'Educational ' if x == True else '')
    maine_current['01_04_02_Financial Services (if reporting to the Department of Professional and Financial Services, this form is not required. 10 M.R.S.A. §1348(5))'] = maine_current['01_04_02_Financial Services (if reporting to the Department of Professional and Financial Services, this form is not required. 10 M.R.S.A. §1348(5))'].apply(lambda x: 'Financial Services ' if x == True else '')
    maine_current['01_04_03_Governmental Entity in Maine'] = maine_current['01_04_03_Governmental Entity in Maine'].apply(lambda x: 'Governmental Entity in Maine ' if x == True else '')
    maine_current['01_04_04_Other Governmental Entity'] = maine_current['01_04_04_Other Governmental Entity'].apply(lambda x: 'Other Governmental Entity ' if x == True else '')
    maine_current['01_04_05_Health Care'] = maine_current['01_04_05_Health Care'].apply(lambda x: 'Health Care ' if x == True else '')
    maine_current['01_04_06_Other Commercial'] = maine_current['01_04_06_Other Commercial'].apply(lambda x: 'Other Commercial ' if x == True else '')
    maine_current['01_04_07_Not-for-Profit'] = maine_current['01_04_07_Not-for-Profit'].apply(lambda x: 'Not-for-Profit ' if x == True else '')
    maine_current['01_04_08_POS Vendor'] = maine_current['01_04_08_POS Vendor'].apply(lambda x: 'POS Vendor ' if x == True else '')
    
    maine_current['Entity Type'] = maine_current['01_04_01_Educational'] + maine_current['01_04_02_Financial Services (if reporting to the Department of Professional and Financial Services, this form is not required. 10 M.R.S.A. §1348(5))'] + maine_current['01_04_03_Governmental Entity in Maine'] + maine_current['01_04_05_Health Care'] + maine_current['01_04_06_Other Commercial'] + maine_current['01_04_07_Not-for-Profit'] + maine_current['01_04_08_POS Vendor']
    
    maine_current['Individuals Affected'] = maine_current['02_01_01_Total number of persons affected (including Maine residents)'] + '(' + maine_current['02_01_02_Total number of Maine residents affected'] + ' in Maine)'

    maine_current['02_02_01_Loss or theft of device or media (e.g., computer, laptop, external hard drive, thumb drive, CD, tape)'] = maine_current['02_02_01_Loss or theft of device or media (e.g., computer, laptop, external hard drive, thumb drive, CD, tape)'].apply(lambda x: 'Loss or theft of device or media (e.g., computer, laptop, external hard drive, thumb drive, CD, tape) ' if x == True else '')
    maine_current['02_02_02_Internal system breach'] = maine_current['02_02_02_Internal system breach'].apply(lambda x: 'Internal system breach ' if x == True else '') 
    maine_current['02_02_03_Insider wrongdoing'] = maine_current['02_02_03_Insider wrongdoing'].apply(lambda x: 'Insider wrongdoing' if x == True else '')
    maine_current['02_02_04_External system breach (e.g., hacking)'] = maine_current['02_02_04_External system breach (e.g., hacking)'].apply(lambda x: 'External system breach (e.g., hacking)' if x == True else '')
    maine_current['02_02_05_Inadvertent disclosure'] =  maine_current['02_02_05_Inadvertent disclosure'].apply(lambda x: 'Inadvertent disclosure' if x == True else '')
    #maine_current['02_02_06_Other'] = maine_current['02_02_06_Other']
    maine_current['02_02_07_If other, specify'] = maine_current['02_02_07_If other, specify'].apply(lambda x: '' if x == None else x)

    maine_current['Type of Breach'] = maine_current['02_02_01_Loss or theft of device or media (e.g., computer, laptop, external hard drive, thumb drive, CD, tape)']+ maine_current['02_02_02_Internal system breach'] + maine_current['02_02_03_Insider wrongdoing'] + maine_current['02_02_04_External system breach (e.g., hacking)']+maine_current['02_02_04_External system breach (e.g., hacking)'] + maine_current['02_02_05_Inadvertent disclosure'] + maine_current['02_02_07_If other, specify']
    
    maine_current['02_03_01_Social Security Number'] = maine_current['02_03_01_Social Security Number'].apply(lambda x: 'Social Security Number ' if x == True else '')
    maine_current["02_03_02_Driver's license number or non-driver identification card number"] = maine_current["02_03_02_Driver's license number or non-driver identification card number"].apply(lambda x: "Driver's license number or non-driver identification card number " if x == True else '')
    maine_current['02_03_03_Financial account number or credit or debit card number, in combination with the security code, access code,  password, or PIN for the account'] = maine_current['02_03_03_Financial account number or credit or debit card number, in combination with the security code, access code,  password, or PIN for the account'].apply(lambda x: 'Financial account number or credit or debit card number, in combination with the security code, access code,  password, or PIN for the account ' if x == True else '')

    maine_current['Data Stolen'] = maine_current['02_03_01_Social Security Number'] + maine_current["02_03_02_Driver's license number or non-driver identification card number"]+maine_current['02_03_03_Financial account number or credit or debit card number, in combination with the security code, access code,  password, or PIN for the account']


    maine_current = maine_current.drop(columns = ['Start Date','01_03_02_Street Address','01_03_03_City', '01_03_04_State', '01_03_05_Zip Code',
       '01_04_01_Educational','01_04_02_Financial Services (if reporting to the Department of Professional and Financial Services, this form is not required. 10 M.R.S.A. §1348(5))',
        '01_04_03_Governmental Entity in Maine','01_04_04_Other Governmental Entity','01_04_05_Health Care','01_04_06_Other Commercial','01_04_07_Not-for-Profit','01_04_08_POS Vendor',
        '01_05_01_Name', '01_05_02_Title',
        '01_05_03_Firm name (if different than entity name)',
        '01_05_04_Telephone Number', '01_05_05_Email Address',
        '01_05_06_Relationship to entity whose information was compromised',
        '02_01_01_Total number of persons affected (including Maine residents)',
        '02_01_02_Total number of Maine residents affected',
        '02_01_03_If the number of Maine residents exceeds 1,000, have the consumer reporting agencies been notified?','02_02_01_Loss or theft of device or media (e.g., computer, laptop, external hard drive, thumb drive, CD, tape)',
        '02_02_02_Internal system breach', '02_02_03_Insider wrongdoing',
        '02_02_04_External system breach (e.g., hacking)',
        '02_02_05_Inadvertent disclosure', '02_02_06_Other',
        '02_02_07_If other, specify','03_01_01_Written', '03_01_02_Electronic', '03_01_03_Telephone',
        '03_01_04_Substitute notice','03_01_06_Attach a copy of the template of the notice to affected Maine residents',
        '03_02_01_Were identify theft protection services offered?',
        '03_02_02_If yes, for what duration?',
        '03_02_03_If yes, by what provider?',
        '03_02_04_If yes, provide a brief description of the service.',
        '02_03_01_Social Security Number',
       "02_03_02_Driver's license number or non-driver identification card number",
       '02_03_03_Financial account number or credit or debit card number, in combination with the security code, access code,  password, or PIN for the account',
       '03_01_07_List dates of any previous (within 12 months) breach notifications'])

    maine_current = maine_current.rename(index=str, columns={'01_03_01_Entity Name':'Name of Entity',
        '02_01_04_Date(s) Breach Occurred':'Dates of Breach', '02_01_05_Date Breach Discovered':'Date(s) of Discovery of Breach', 
        '03_01_05_Date(s) of consumer notification':'Date Notice Provided to Consumers'})


    #print ('NEED TO HANDLE MAINE CURRENT AND merge')
    #print (maine_current.head())
    archived_breaches = core_year_url + year_url[1][5:]
        
        
    resp = requests.get(archived_breaches)
    output = open('Maine.xls', 'wb')
    output.write(resp.content)
    output.close()

    df = pd.read_excel('Maine.xls') 
    os.remove("Maine.xls")

    headers = df.iloc[0] #takes first row (needed hearders)
    maine_arch = pd.DataFrame(df.values[1:], columns=headers) #creates new pd data frame with the proper headers and fills data
    maine_arch = maine_arch.dropna(how = 'all')
    maine_arch = maine_arch.drop(columns = ['Company Contact Information','Attorney                   (If Represented)'])

    maine_arch = maine_arch.rename(index=str, columns={"Company Whose      Data Was Breached": "Name of Entity",
        'Date of Breach':'Dates of Breach',
        'Date of Notification':'Date Notice Provided to Consumers', 'Type of Information':'Data Stolen', 
        'Number of Maine Residents Affected':'Individuals Affected'})

    maine_arch['Individuals Affected'] = maine_arch['Individuals Affected'].apply(lambda x: str(x) + '(in Maine')

    maine = pd.concat([maine_current, maine_arch], ignore_index = True)
    maine['State Reported'] = 'Maine'
    maine['Source(s)'] = 'https://www.maine.gov/ag/consumer/identity_theft/'
    maine = maine.drop(columns = ['Entity Address'])
    
    try:
        maine = maine[:int(maine.index[maine['Name of Entity'] == most_recent_breach][0])+1]
    except:
        pass
    most_recent_breach = maine['Name of Entity'].iloc[0]
    
    return maine, most_recent_breach

def update_Maryland(most_recent_breach):
    def base_url(year):
        return "http://www.marylandattorneygeneral.gov/_layouts/15/inplview.aspx?List=%7B04EBF6F4-B351-492F-B96D-167E2DE39C85%7D&View=%7BAC628F51-0774-4B71-A77E-77D6B9909F7E%7D&ViewCount=68&IsXslView=TRUE&IsCSR=TRUE&ListViewPageUrl=http%3A%2F%2Fwww.marylandattorneygeneral.gov%2FPages%2FIdentityTheft%2Fbreachnotices.aspx&GroupString=%3B%23{year}%3B%23&IsGroupRender=TRUE&WebPartID={{AC628F51-0774-4B71-A77E-77D6B9909F7E}}".format(year=year)

    def next_url(url):
        # Pull out the parameters from `NextHref`
        params = urllib.parse.parse_qs(urlparse(url).query)
        del params["View"] # Remove this parameter
        # Encode the parameters so we can put it back in the URL template
        params = urllib.parse.urlencode(params, doseq=True)
        return "http://www.marylandattorneygeneral.gov/_layouts/15/inplview.aspx?List=%7B04EBF6F4-B351-492F-B96D-167E2DE39C85%7D&View=%7BAC628F51-0774-4B71-A77E-77D6B9909F7E%7D&ViewCount=91&IsXslView=TRUE&IsCSR=TRUE&ListViewPageUrl=http%3a%2f%2fwww.marylandattorneygeneral.gov%2fPages%2fIdentityTheft%2fbreachnotices.aspx&{params}&IsGroupRender=TRUE&WebPartID={{AC628F51-0774-4B71-A77E-77D6B9909F7E}}".format(params=params)

    def next_id(url):
        params = urllib.parse.parse_qs(urlparse(url).query)
        return params['p_ID']

    def request(url):
   # The POST request with referrer (and without the `pass`) in the following line
   
   # IS THIS THE LINE YOU'RE REFFERING TO? 

   # should URL be baseurl
        result = requests.post(url, headers={'referer': "http://www.marylandattorneygeneral.gov/Pages/IdentityTheft/breachnotices.aspx"})
        return result.text
    maryland = pd.DataFrame()
    for year in range(2015, 2020):
        rows = []
        seen_ids = []
        response = json.loads(request(base_url(year)))
   # `response` is a dictionary, with the `NextHref` key pointing to URL we need to use next and `Row` holding the data we need
        for x in response['Row']:
                rows.append({
                    'case_title': x['Case_x0020_Title'],
                    'case_number': x['FileLeafRef.Name'],
                    'date_received': x['Date_x0020_Received'],
                    'how_breach_occurred': x['How_x0020_Breach_x0020_Occurred'],
                    'no_of_md_residents': x['No_x0020_of_x0020_MD_x0020_Residents']
                })
        while("NextHref" in response):
            seen_ids.append(next_id(response['NextHref']))
            response = json.loads(request(next_url(response["NextHref"])))
            if 'NextHref' in response and next_id(response['NextHref']) in seen_ids:
                break
        #TOOK FROM ABOVE CODE to get specific data
            for x in response['Row']:
                rows.append({
                    'case_title': x['Case_x0020_Title'],
                    'case_number': x['FileLeafRef.Name'],
                    'date_received': x['Date_x0020_Received'],
                    'how_breach_occurred': x['How_x0020_Breach_x0020_Occurred'],
                    'no_of_md_residents': x['No_x0020_of_x0020_MD_x0020_Residents']
                })
        rows = rows[::-1]
        maryland = maryland.append(json_normalize(rows))


    #maryland = json_normalize(rows).to_csv("maryland.csv")
    maryland['no_of_md_residents'] = maryland['no_of_md_residents'].fillna('Unknown')
    maryland['no_of_md_residents'] = maryland['no_of_md_residents']+' (in Maryland)'
    maryland = maryland.drop(columns = ['case_number'])
    maryland = maryland.rename(index=str, columns={"case_title": "Name of Entity", 'no_of_md_residents':'Individuals Affected', 
        'date_received': "Dates of Breach", 'how_breach_occurred': 'Type of Breach'})
    maryland['State Reported'] = 'Maryland'
    maryland['Source(s)'] = 'http://www.marylandattorneygeneral.gov/Pages/IdentityTheft/breachnotices.aspx'

    maryland["Start Date of Breach"] = maryland['Dates of Breach'].apply(guess_date_mm_dd_yyyy)
    new_most_recent_breach = maryland['Name of Entity'].iloc[-1]
    maryland = maryland.reset_index(drop=True)
    try:
        maryland = maryland[int(maryland.index[maryland['Name of Entity'] == most_recent_breach][-1])-1:]
    except:
        pass

    return maryland, new_most_recent_breach
def update_Massachusetts(most_recent_breach):
    pass

def update_Montana(most_recent_breach):
    driver = webdriver.Chrome()  # Optional argument, if not specified will search path.
    driver.get('https://dojmt.gov/consumer/consumers-known-data-breach-incidents/#')

    tables = []


    while True:
        page = driver.page_source
        soup = BeautifulSoup(page, "lxml")
        html_table = soup.find("table")
        h = html_table.find("tr", class_="footable-filtering")
        h.extract()
        f = html_table.find("tfoot")
        f.extract()
        table = pd.read_html(str(html_table))
        links = [link for link in map(lambda x: (x.find("a").get("href") if x.find("a") else ""), html_table.find_all('td', class_="ninja_clmn_nm_notificationdocuments"))]

        table[0]['Link to PDF'] = links
        tables.append(table)
    
        try:
            disabled_button = driver.find_element_by_css_selector("div.footable-page-nav.disabled")
        except:
            try:
#             button = driver.find_element_by_css_selector("#footable_52829 > tfoot > tr > td > div > ul > li:nth-child(68) > a")
                if driver.find_element_by_css_selector("[data-page='next']").get_attribute("class") == "footable-page-nav disabled":
                    break
                button = driver.find_element_by_css_selector("[data-page='next'] > a")
                button.click()
            except:
                break
        #button varriable is where the driver is supposed to click on the csv link, and download it.
        #it does download the csv b

        # button = driver.find_element_by_css_selector("table_1_wrapper > div.dt-buttons > a.dt-button.buttons-csv.buttons-html5.DTTT_button.DTTT_button_csv > span")
        time.sleep(1)
    driver.quit()

        #button varriable is where the driver is supposed to click on the csv link, and download it.
        #it does download the csv b

        # button = driver.find_element_by_css_selector("table_1_wrapper > div.dt-buttons > a.dt-button.buttons-csv.buttons-html5.DTTT_button.DTTT_button_csv > span")

    montana = pd.concat(map(lambda x: x[0], tables))
    montana = montana.reset_index(drop=True)
    montana['MONTANANS AFFECTED'] = montana['MONTANANS AFFECTED'].apply(lambda x: str(x) + ' in Montana')
    montana['Dates of Breach'] = montana['START OF BREACH'] + '-' + montana['END OF BREACH']



    montana = montana.drop(columns = ['NOTIFICATION DOCUMENTS','START OF BREACH','END OF BREACH'])
    montana = montana.rename(index=str, columns={"BUSINESS NAME": "Name of Entity", 'MONTANANS AFFECTED':'Individuals Affected', 'DATE REPORTED': "Reported Date"})
    montana['State Reported'] = 'Montana'
    montana['Source(s)'] = 'https://dojmt.gov/consumer/consumers-known-data-breach-incidents/#'

    split_date = montana['Dates of Breach'].apply(lambda x: re.split('-', str(x)))
    montana["Start Date of Breach"] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[0]))
    montana['End Date of Breach'] = split_date.apply(lambda x: guess_date_mm_dd_yyyy(x[1]) if len(x) > 1 else None)
    montana['Reported Date'] = montana['Reported Date'].apply(guess_date_mm_dd_yyyy)

    try:
        montana = montana[:int(montana.index[montana['Link to PDF'] == most_recent_breach][0])+1]
    except:
        montana = montana

    most_recent_breach = montana['Link to PDF'][0]
    return montana, most_recent_breach