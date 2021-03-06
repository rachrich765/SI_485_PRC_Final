from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from sklearn import model_selection, preprocessing, naive_bayes
from sklearn.feature_extraction.text import CountVectorizer
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from pandas.io.json import json_normalize
from six.moves import cPickle as pickle
from nltk.corpus import stopwords
from collections import Counter
from selenium import webdriver
from bs4 import BeautifulSoup
from tabula import read_pdf
from pathlib import Path
import PyPDF2
import lxml.html as lh
import pandas as pd
import numpy as np
import requests
import datetime
import sklearn
import string
import json
import nltk
import csv
import time
import re
import os


def download_parse_file(url):
    print ('downloading')
    # Get the contents of the pdf file
    try:
      response = requests.get(url)
      my_raw_data = response.content
    except:
      return ''

    # Write the contents into a temporary file
    with open("new_pdf.pdf", 'wb') as my_data:
        my_data.write(my_raw_data)
    my_data.close()
    try:
        pdf_file = open(my_data, 'rb')
        read_pdf = PyPDF2.PdfFileReader(pdf_file)
        number_of_pages = read_pdf.getNumPages()
        page_content = []
        for i in range(0, number_of_pages):
            page = read_pdf.getPage(i)
            page = page.extractText()
            page = page.lower()
            page_content.append(page)
        page_content = ''.join(page_content)
    except:
        page_content = "Unreadable File" 
    # Try to convert the contents of the pdf to a string     
    #try:
    #    file = textract.process('new_pdf.pdf', method='pdfminer')
    #except:
    #    file = "Unreadable File"   

    # Delete the temporary file
    os.remove("new_pdf.pdf")

    # Return the string contents of the pdf
    return str(page_content)

#Clean Dates Columns

#df.to_csv('data_breach_chronology_new_dates.csv')



def find_datatype_breached(pdf_text):
    datatypes = ['social security number', "driver's license", "passport", "state id", "license",
                               'name', 'date of birth', 'dates of birth', "gender", 
                               'credit card', "debit card", "payment",
                               "financial", "security code", "verification code", "cvv", "date of card expiration", "expiration date",
                               "login", "password", "user name", "username", "log in"  
                               "telephone number", "email", "phone number", "address",
                               "medical", "blood type", "donor profile",
                               "insurance",
                                "profile", "id number", "identification number",
                              "ip address"]
    negation = ['no','not',"wasn't"]
    sentences = pdf_text.split('.')
    sents = []
    for sentence in sentences:
        for item in datatypes:
            if item in sentence:
                sents.append(sentences.index(sentence))

    del_ind = []
    for ind in sents:
        for item in negation: 
            if item in sentences[ind]:
                del_ind.append(ind)

    del_ind = sorted(set(del_ind), reverse=True)

    for ind in del_ind:
        del sentences[ind]
    k = '.'.join(sentences)
    stolen_list = [x for x in datatypes if x in k]
    if len(stolen_list) == 0:
        stolen_list = "UNK"
    return stolen_list


def clean_new_data(dataframe):
  df = dataframe

  #Download and parse PDF and find datatype breached
  df['PDF text'] = df['Link to PDF'].apply(download_parse_file)
  df['Data Stolen'] = df['PDF text'].apply(find_datatype_breached)

  return df

#Open the classifier
with open('nb_breach_type_large.pkl', 'rb') as fid:
    l_nb_loaded = pickle.load(fid)
l_vecs = pd.read_csv('large_count_vectors.csv')

# Use word count method on type of breach column to classify type of breach
def get_breach_type_cause(large_df):
    master = large_df
    causes_sorted_1 = []
    causes = list(master["Type of Breach"].fillna("NONE"))
    words_for_1 = ['unauthorized', 'fraud', 'attack', 'malicious', 'compromise', 'suspicious', 'malware', 'ransomware', 
               'cyber', 'authorization', 'phishing', 'cybersecurity', 'infected', 'compromised',
               'virus', 'compromise', 'attacked', 'hacked', 'spoofing', 'scam', 'scammer', 'network', 'fraudulent', 'email',
               'e-mail', 'emails', 'e-mails', 'phish', 'attack', 'without', 'cyberattack', 'fraudster', 'discovered',
               'system', 'systems', 'third-party', 'third', 'party']
    words_for_2 = ['contractor', "inside", 'insider', 'former']
    words_for_3 = ['papers', 'paper', 'letter']
    words_for_4 = ["laptop", "phone", 'hard', 'drive', 'laptops', 'car', 'cars', 'theft']
    words_for_5 = ["computer", "server"]
    words_for_6 = ["inadvertently", 'mistake', 'accident', 'mistakenly', 'mistaken', 'accidentally', 'improper', 'disposal']
    try: 
        for cause in causes:
            cause = str(cause)
            if cause == 'NONE':
                causes_sorted_1.append("UND")
                continue
            cause = cause.lower()
            cause = re.sub('[^\w\s]','',cause)
            cause = cause.split(' ')
            a = [x for x in cause if x in words_for_1]
            b = [x for x in cause if x in words_for_2]
            c = [x for x in cause if x in words_for_3]
            d = [x for x in cause if x in words_for_4]
            e = [x for x in cause if x in words_for_5]
            f = [x for x in cause if x in words_for_6]
            tup = (len(a),len(b),len(c),len(d),len(e),len(f))
            if max(tup) != 0:
                causes_sorted_1.append(tup.index(max(tup))+1)
            if max(tup) == 0: 
                causes_sorted_1.append(7)
    except:
        causes_sorted_1.append("UND")
            
    return causes_sorted_1


def clean_pdf_text(pdf_text):
    modified = stopwords.words('english')
    modified.extend(['information','happened', 'january', 'february', 'march', 'april',
                'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'])
#     print(modified)
    text = pdf_text
#     print(text)
    t = re.sub(r'[^\w\s]','',text)
    t = re.sub("[^A-z\s]",'',t)
    t = re.sub('\?','',t)
    t = re.sub(r'[0-9]','',t)
    k = t.split(' ')
#     print(k)
#     k = list(filter(None, k))
    no_stop = [x for x in k if x not in modified]
    b = [item.strip() for item in no_stop]
    a = [item for item in b if item != '']
    no_st = ' '.join(a)
    return no_st

# Use machinee learning method on PDF text to classify type of breach
def get_breach_type_classifier(large_df, l_vecs = l_vecs, l_nb = l_nb_loaded):
    causes_sorted_2 = []
    words_for_1 = ['unauthorized', 'fraud', 'attack', 'malicious', 'compromise', 'suspicious', 'malware', 'ransomware', 
               'cyber', 'authorization', 'phishing', 'cybersecurity', 'infected', 'compromised',
               'virus', 'compromise', 'attacked', 'hacked', 'spoofing', 'scam', 'scammer', 'network', 'fraudulent', 'email',
               'e-mail', 'emails', 'e-mails', 'phish', 'attack', 'without', 'cyberattack', 'fraudster', 'discovered',
               'system', 'systems', 'third-party', 'third', 'party', 'hacking'
              ]
    words_for_2 = ['contractor', 'insider', 'former', 'employee']
    words_for_3 = ['papers', 'paper', 'letter']
    words_for_4 = ["laptop", "phone", 'hard', 'drive', 'laptops', 'car', 'cars', 'stolen']
    words_for_5 = ["computer", "server"]
    words_for_6 = ["inadvertently", 'mistake', 'accident', 'mistakenly', 'mistaken', 'accidentally']

    others = words_for_2 + words_for_3 + words_for_4 + words_for_5 + words_for_6 
    trig = others+words_for_1
    pdfs = list(large_df["PDF text"].fillna("no_pdf"))
    for pdf in pdfs:
        if pdf == "no_pdf":
            causes_sorted_2.append("UND")
        else:
            p = clean_pdf_text(pdf)  
            c = p.split()
            if len(c)<=66:
                causes_sorted_2.append(7)
                continue
            a = Counter(c)
#             print(a)
            features = []  
            k = list(l_vecs.columns)[1:-1]
            for word in k:
                if word in a:
                    features.append(a[word])
                else:
                    features.append(0)
#             print(features)    
            feat_list = features
            features = np.array(features)
            for feat in a:
#                 print(feat)
                try:
                    if feat in trig:
#                         print('multi')
                        i = k.index(feat)
                        features[i] = features[i]*1000
#                         print(features[i])
                except:
                    pass
            prediction = l_nb.predict(features.reshape(1,-1))
            prediction = int(prediction)
            if prediction == 0:
                causes_sorted_2.append(1)
            else:
                pdf = p
                p = p.split(' ')
                words_for_1 = ['unauthorized', 'fraud', 'attack', 'malicious', 'compromise', 'suspicious', 
                               'malware', 'ransomware', 
               'cyber', 'authorization', 'phishing', 'cybersecurity', 'infected', 'compromised',
               'virus', 'compromise', 'attacked', 'hacked', 'spoofing', 'scam', 'scammer', 'network', 'fraudulent', 
                'phish', 'attack', 'cyberattack', 
                'fraudster', 'third-party', 'hacking'
              ]
                a = [x for x in c if x in words_for_1]
#                 print(a)
                b = [x for x in p if x in words_for_2]
#                 print(b)
                c = [x for x in p if x in words_for_3]
                d = [x for x in p if x in words_for_4]

                e = [x for x in p if x in words_for_5]
                f = [x for x in p if x in words_for_6]
#                 print(b)
                tup = (len(a),len(b),len(d),len(f))
                opt = [1,2,4,6]
                if len(f)!= 0:
                    causes_sorted_2.append(6)
                elif 'car' in d:
                    causes_sorted_2.append(4)
                elif 'contractor' in pdf:
                    causes_sorted_2.append(2) 
                elif 'paper' in c:
                    causes_sorted_2.append(3)
#                 elif 'stolen' in e or 'theft' in e:
#                     causes_sorted_2.append(5)
#                 elif 'former employee' in pdf:
#                     print(pdf)
#                     causes_sorted_2.append(2)
                elif 'stolen' in pdf and 'computer' in e or 'server' in e:
                    causes_sorted_2.append(5)
                elif max(tup) != 0:
                    causes_sorted_2.append(opt[tup.index(max(tup))])
                elif max(tup) == 0: 
                    causes_sorted_2.append(7)
                    
            
    return causes_sorted_2


#combine the results of both breach type methods and retain the more effective
def final_list(l1, l2):
    assert len(l1) == len(l2)
    final = []
    for i in range(0, len(l1)):
        item = l1[i]
        if item != 'UND':
            final.append(item)
        else:
            m = l2[i]
            if type(m) == int:
                final.append(m)
            elif m == "UND":
                final.append(7)
    return final 