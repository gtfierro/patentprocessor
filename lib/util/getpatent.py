#!/usr/bin/env python

import sys
import re
import time
import mechanize
from BeautifulSoup import BeautifulSoup

if len(sys.argv) < 2:
    print "Given a patent id number, will download the relevant zipfile"
    print "Usage: ./getpatent.py <patent id number>"
    print "Example: ./getpatent.py 7783348"
    sys.exit(0)

patent_name = sys.argv[1]

if patent_name[:2].upper() != 'US':
    patent_name = 'US'+patent_name

BASE_URL = 'http://www.google.com/patents/'
ZIP_BASE_URL = 'http://commondatastorage.googleapis.com/patents/grant_full_text/'
br = mechanize.Browser()
br.addheaders = [('User-agent', 'Feedfetcher-Google-iGoogleGadgets;\
 (+http://www.google.com/feedfetcher.html)')]
br.set_handle_robots(False)
html = br.open(BASE_URL+patent_name).read()

print 'Got HTML for patent page'

soup = BeautifulSoup(html)
sidebar = soup.find('div', {'class': 'patent_bibdata'})
text = str(sidebar.text)
date = re.search(r'(?<=Issue date: )[A-Za-z]{3} [0-9]{1,2}, [0-9]{4}', text).group()
date_struct = time.strptime(date, '%b %d, %Y')
year = str(date_struct.tm_year)[2:]
month = str(date_struct.tm_mon).zfill(2)
day = str(date_struct.tm_mday).zfill(2)

zipfile = 'ipg{0}{1}{2}.zip'.format(year,month,day)

zipurl = '{0}{1}/{2}'.format(ZIP_BASE_URL,date_struct.tm_year,zipfile)

print 'Downloading ZIP file: ',zipurl

res = br.retrieve(zipurl, zipfile)
print res

print 'Finished downloading'
