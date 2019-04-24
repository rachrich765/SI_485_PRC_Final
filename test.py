import requests
import PyPDF2
import os
from pathlib import Path

def download_parse_file(url):
    print ('downloading')
    # Get the contents of the pdf file
    my_raw_data = Path('new_pdf.pdf')
    response = requests.get(url)
    my_raw_data.write_bytes(response.content)
    #print (my_raw_data)
    # Write the contents into a temporary file
    #with open("new_pdf.pdf", 'wb') as my_data:
    #    my_data.write(my_raw_data)
    #my_data.close()
    pdf_file = open(my_raw_data, 'rb')
    read_pdf = PyPDF2.PdfFileReader(pdf_file)
    number_of_pages = read_pdf.getNumPages()
    page_content = []
    for i in range(0, number_of_pages):
        page = read_pdf.getPage(i)
        page = page.extractText()
        page = page.lower()
        page_content.append(page)
    page_content = ''.join(page_content)
    print (page_content)
    
    # Try to convert the contents of the pdf to a string     
    #try:
    #    file = textract.process('new_pdf.pdf', method='pdfminer')
    #except:
    #    file = "Unreadable File"   

    # Delete the temporary file
    os.remove("new_pdf.pdf")

    # Return the string contents of the pdf
    return str(page_content)

print(download_parse_file('https://oag.ca.gov/system/files/Customer%20Notice_0.pdf'))