# UMSI | PRC Data Breach Chronology Importer
This tool contains code to import the publically accessible records of data breaches that have occured in the United States. It merges all of that data together and supplements it with additional data parsed from PDFs and gathered via the wikipedia API.
## Getting Started and Deployment 
To use this tool, once the prerequisites are installed, run python main.py from the terminal application. To do so, open the terminal application on your mac computer. You will then need to navigate to the directory where the file is stored using the cd (change directory) command. When you are in the folder, run python main.py to trigger the program. It will output various csv files (discussed at the end of the document).
In order to automate this entirely, we developed a method using cron. Cron is a time-based job scheduler that allows you to automate the process of running the code (which will update the csv file).You can follow the instructions below to set this up, or you can alternatively run the main.py file whenever you want to update it.
In order to create your first cron job, type the following in terminal:
crontab -e

Once you are ready to create the job. Add the following into the file (update file path as needed). This will run the code every day at 2:30 am (the time can be set as desired).
30 2 * * * /path_to_directory/SI_485_PRC_Final/main.py

Once complete, type Shift-z-z and your job will be created. Type the following to verify it has been created.
crontab -l

## Prerequisites
The following packages must be installed prior to running the program. [UMSI will hold a meeting with Privacy Rights Clearinghouse to help facilitate downloads].
- datetime
	- pip install datetime
- Pandas
	- pip install pandas
- Regex
	- pip install regex
- Textract
	- pip install homebrew
	- brew cask install xquartz 
	- brew install poppler antiword unrtf tesseract swig 
	- pip install textract
	- brew cask install pdftotext
- Scikit learn
	- pip install scikit-learn
- Selenium
	- pip install selenium
- ChromeDriver
	- We have included this in the github repository 
- Tabula
	- pip install tabula-py
- Beautiful Soup
	- pip install beautifulsoup4
- Requests 
	- pip install requests
- Wikipedia
	- pip install wikipedia

## Files

### main.py
This is the file that needs to be run to update the data_breach_chronology_new.csv file.
It imports the functions and files within the repository so that only one file needs to be run.
For each state, it runs the update function (described below) and captures the new data in a pandas dataframe. It also saves the state’s raw data in another csv as a backup. An example of the process for California is provided here.
 
	California
	ca_df, ca_recent = update_California(recent['recent']['California'])
	recent['recent']['California'] = ca_recent
	if os.path.isfile('california.csv'):
		california = pd.read_csv('california.csv')
		ca_df.append(california,ignore_index=True).to_csv('california.csv', index = False)
	else:
		ca_df.to_csv('california.csv', index = False)
	new_this_run = new_this_run.append(ca_df,ignore_index=True)
	print ('\n', 'Fetched California Data', '\n')
 
It then access the functions from the files below to merge, clean and supplement the data. When run, the csv file is updated.

### functions.py
This file contains the functions used to scrape the data from each of the 14 websites. 
There are 14 functions called update_(state/government entity name). The only inputs required for the functions are the data saved about the most recently recorded data breach for each state so that only new data is processed. When triggered, it runs, collects new data and returns it in a pandas dataframe.

The other functions included in this file are functions that perform actions that were commonly used throughout the update functions.
	basic_beautiful_soup: Takes a url and returns html contents of the page
	
	download_parse_file: Takes a link to a pdf and returns the text contents if possible
	
	guess_date_dashesandslashes: takes date in format mm/dd/yy or mm-dd-yy and returns date in datetime format
	
	guess_date_mm_dd_yyyy: takes date in format mm/dd/yyyy and returns date in datetime format
	
	guess_date_yyyy_mm_dd: takes date in format yyyy-mm-dd and returns date in datetime format
	
	guess_date_mm_dd_yy: takes date in format mm/dd/yy and returns date in datetime format
	
	date_w_day: takes date in format day, mm dd, yyyy and returns date in datetime format
	
	guess_date_month_dd_yyyy: takes date in format mm dd, yyyy and returns date in datetime format
### Clean_new.py 

This file contains the code that is applied to clean all new data breaches being added to the chronology.

	download_parse_file(url): Input - a pdf url. Output - the downloaded string of the pdf.
	
	find_datatype_breached: Input - string text of pdf. Output - the types of data breached, found through word counting. The datatypes list can be modified to include more potential information that has been breached. 
	
	clean_new_data: Input - incomplete dataframe (without pdf text and datatype stolen). Output - completed dataframe with pdf text and datatype stolen.
	
	get_breach_type_cause: Uses word counting method on rows containing brief description of breach type to infer the PRC classification of the breach type. 

	clean_pdf_text: Input - raw pdf text. Output - cleaned version of pdf text. Used prior to operating on pdfs for classification purposes. 

	get_breach_type_classifier: Input - pdf text. Output - classified cause of breach from pdf text. Classifier file nb_breach_type_large.pkl is used to determine whether or not the breach was hacking. Following this, word counting is used to infer the cause if it was not hacking. 

### Clean_all.py 
This file contains the code that can be run on the entire csv file to clean the Name of Entity column and to remove duplicates.
	
	clean_ents: cleans the entities in the final csv, as the same entity may be listed under multiple names. Input - dataframe. Output - cleaned dataframe on entity name. 


	remove_duplicates: groups data breach records by the Name of Entity and Start Date of Breach columns and combines them into a single record that retains all of the data.

### Add_info.py

Contains the functions needed to add information from the wikipedia API.

It takes the “Name of Entity” column and for each breach it:
Check if there is a wikipedia page for the entity 
Attempts to gather the industry, parent company and website url
Adds the information to the data breach chronology dataframe

### nb_breach_type_large.pkl

This is the classifier invoked in the clean_new.py file. It differentiates between hacking and non-hacking attacks using count vectors. 

### large_count_vectors.csv 

These vectors are used to assist in classification of breach type in clean_new.py file

### Chromedriver 

This file allows the chrome driver, an open source tool, that allows for JavaScript execution on websites that utilize JavaSript in order for easy data scraping. 

### data_chronology_importer_new.csv 

This is the final csv that contains all of the collected data breaches. It has been cleaned and supplemented with additional information. The number of rows will continually change, but should always exceed 9933 rows (as of 4/18/19). The columns are:
- Name of Entity 
- Start Date of Breach
- End Date of Breach 
- Individuals Affected 
- State Reported 
- Data Stolen
- Date Notice Provided to Consumers 
- Date(s) of Discovery of Breach 
- Dates of Breach 
- Industry 
- Parent Company 
- Website 
- Link to PDF 
- Location of Breached Information 
- Reported Date 
- Type of Breach
- Source(s)

### Recent.csv

This file contains a row for each of the sources of data. In each row, there is data on the last breach collected for that state. The format varies by state based on the available data. 

### [SOURCE_NAME].csv

Each data source has a separate csv file with the raw data collected from their state. This data has not been cleaned or supplemented. It is a backup of the date.

## Authors

Julie Burke
Zi Huang
Sam Karmin 
Rachel Richardson

## Acknowledgments

sThis project was completed with guidance from our course instructors (Professor Kevyn Collins-Thompson and Ryan Burton) of SI485 at the University of Michigan School of Information.
We worked closely with our client, Emory Roane, from Privacy Rights Clearinghouse
