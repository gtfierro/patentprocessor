import importlib
from ConfigParser import ConfigParser

defaults = {'parse': 'defaultparse',
            'clean': 'True',
            'consolidate': 'True',
            'datadir': '/data/patentdata/patents/2013',
            'dataregex': 'ipg\d{6}.xml',
            'years': None,
            'downloaddir' : None}

def extract_process_options(handler):
    """
    Extracts the high level options from the [process] section
    of the configuration file. Returns a dictionary of the options
    """
    result = {}
    result['parse'] = handler.get('process','parse')
    result['clean'] = handler.get('process','clean') == 'True'
    result['consolidate'] = handler.get('process','consolidate') == 'True'
    result['outputdir'] = handler.get('process','outputdir')
    return result

def extract_parse_options(handler, section):
    """
    Extracts the specific parsing options from the parse section
    as given by the [parse] config option in the [process] section
    """
    options = {}
    options['datadir'] = handler.get(section,'datadir')
    options['dataregex'] = handler.get(section,'dataregex')
    options['years'] = handler.get(section,'years')
    options['downloaddir'] = handler.get(section,'downloaddir')
    if options['years'] and options['downloaddir']:
        options['datadir'] = options['downloaddir']
    return options

def get_config_options(configfile):
    """
    Takes in a filepath to a configuration file, returns
    two dicts representing the process and parse configuration options.
    See `process.cfg` for explanation of the optiosn
    """
    handler = ConfigParser(defaults)
    handler.read(configfile)
    process_config = extract_process_options(handler)
    parse_config = extract_parse_options(handler, process_config['parse'])
    return process_config, parse_config

def get_dates(yearstring):
    """
    Given a [yearstring] of forms
    year1
    year1-year2
    year1,year2,year3
    year1-year2,year3-year4
    Creates tuples of dates
    """
    years = []
    for subset in yearstring.split(','):
        if subset == 'default':
            years.append('default')
            continue
        sublist = subset.split('-')
        # left-justify the strings with 0s to add support
        # for days and weeks in the date
        start = int(sublist[0].ljust(8,'0'))
        end = int(sublist[1].ljust(8,'0')) if len(sublist) > 1 else float('inf')
        years.append((start,end))
    return years


def get_xml_handlers(configfile, document_type='grant'):
    """
    Called by parse.py to generate a lookup dictionary for which parser should
    be used for a given file
    """
    handler = ConfigParser()
    handler.read(configfile)
    xmlhandlers = {}
    config_item = 'grant-xml-handlers' if document_type == 'grant' \
                   else 'application-xml-handlers'
    for yearrange, handler in handler.items(config_item):
        for year in get_dates(yearrange):
            try:
                xmlhandlers[year] = importlib.import_module(handler)
            except:
                importlib.sys.path.append('..')
                xmlhandlers[year] = importlib.import_module(handler)
    return xmlhandlers
