import re
import os
import sys
import parse
import time
import itertools
import datetime
import logging
import requests
import zipfile
import cStringIO as StringIO
from bs4 import BeautifulSoup as bs
import lib.alchemy as alchemy

sys.path.append('lib')
from config_parser import get_config_options

logfile = "./" + 'xml-parsing.log'
logging.basicConfig(filename=logfile, level=logging.DEBUG)

def get_year_list(yearstring):
    """
    Given a [yearstring] of forms
    year1
    year1-year2
    year1,year2,year3
    year1-year2,year3-year4
    Expands into a list of year integers, and returns
    """
    years = []
    for subset in yearstring.split(','):
        if subset == 'latest':
            years.append('latest')
            continue
        sublist = subset.split('-')
        start = int(sublist[0])
        end = int(sublist[1])+1 if len(sublist) > 1 else start+1
        years.extend(range(start,end))
    return years

def generate_download_list(years, doctype='grant'):
    """
    Given the year string from the configuration file, return
    a list of urls to be downloaded
    """
    if not years: return []
    urls = []
    link = 'https://www.google.com/googlebooks/uspto-patents-grants-text.html'
    if doctype == 'application':
        link = 'https://www.google.com/googlebooks/uspto-patents-applications-text.html'
    url = requests.get(link)
    soup = bs(url.content)
    years = get_year_list(years)

    # latest file link
    if 'latest' in years:
        a = soup.h3.findNext('h3').findPrevious('a')
        urls.append(a['href'])
        years.remove('latest')
    # get year links
    for year in years:
        header = soup.find('h3', {'id': str(year)})
        a = header.findNext()
        while a.name != 'h3':
            urls.append(a['href'])
            a = a.findNext()
    return urls

def download_files(urls):
    """
    [downloaddir]: string representing base download directory. Will download
    files to this directory in folders named for each year
    Returns: False if files were not downloaded or if there was some error,
    True otherwise
    """
    import os
    import requests
    import zipfile
    import cStringIO as StringIO
    if not (downloaddir and urls): return False
    complete = True
    print 'downloading to',downloaddir
    for url in urls:
        filename = url.split('/')[-1].replace('zip','xml')
        if filename in os.listdir(downloaddir):
            print 'already have',filename
            continue
        print 'downloading',url
        try:
            r = requests.get(url)
            z = zipfile.ZipFile(StringIO.StringIO(r.content))
            print 'unzipping',filename
            z.extractall(downloaddir)
        except:
            print 'ERROR: downloading or unzipping',filename
            complete = False
            continue
    return complete

def run_parse(files, doctype='grant'):
    import parse
    import time
    import sys
    import itertools
    import lib.alchemy as alchemy
    import logging
    logfile = "./" + 'xml-parsing.log'
    logging.basicConfig(filename=logfile, level=logging.DEBUG)
    parse.parse_files(files, doctype)

def run_clean(process_config):
    if process_config['clean']:
        print 'Running clean...'
        import clean

def run_consolidate(process_config):
    if process_config['consolidate']:
        print 'Running consolidate...'
        import consolidate

if __name__=='__main__':
    s = datetime.datetime.now()
    # accepts path to configuration file as command line option
    process_config, parse_config = get_config_options(sys.argv[1])
    doctype = process_config['doctype']

    # download the files to be parsed
    urls = []
    should_process_grants = doctype in ['all', 'grant']
    should_process_applications = doctype in ['all', 'application']
    if should_process_grants:
        urls += generate_download_list(parse_config['years'], 'grant')
    if should_process_applications:
        urls += generate_download_list(parse_config['years'], 'application')
    downloaddir = parse_config['downloaddir']
    if downloaddir and not os.path.exists(downloaddir):
        os.makedirs(downloaddir)
    print 'Downloading files at {0}'.format(str(datetime.datetime.today()))
    download_files(urls)
    print 'Downloaded files:',parse_config['years']
    f = datetime.datetime.now()
    print 'Finished downloading in {0}'.format(str(f-s))

    # find files
    print "Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()),parse_config['datadir'])
    if should_process_grants:
        files = parse.list_files(parse_config['datadir'],parse_config['grantregex'])
        print 'Running grant parse...'
        run_parse(files, 'grant')
        f = datetime.datetime.now()
        print "Found {2} files matching {0} in directory {1}"\
                .format(parse_config['grantregex'], parse_config['datadir'], len(files))
    if should_process_applications:
        files = parse.list_files(parse_config['datadir'],parse_config['applicationregex'])
        print 'Running application parse...'
        run_parse(files, 'application')
        f = datetime.datetime.now()
        print "Found {2} files matching {0} in directory {1}"\
                .format(parse_config['applicationregex'], parse_config['datadir'], len(files))
    print 'Finished parsing in {0}'.format(str(f-s))

    # run extra phases if needed, then move output files
    run_clean(process_config)
    run_consolidate(process_config)
    parse.move_tables(process_config['outputdir'])
