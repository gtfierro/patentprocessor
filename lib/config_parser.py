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
        if subset == 'default':
            years.append('default')
            continue
        sublist = subset.split('-')
        start = int(sublist[0])
        end = int(sublist[1])+1 if len(sublist) > 1 else start+1
        years.extend(range(start,end))
    return years


def get_xml_handlers(configfile):
    """
    Called by parse.py to generate a lookup dictionary for which parser should
    be used for a given file
    """
    handler = ConfigParser()
    handler.read(configfile)
    xmlhandlers = {}
    for yearrange, handler in handler.items('xml-handlers'):
        for year in get_year_list(yearrange):
            try:
                xmlhandlers[year] = importlib.import_module(handler)
            except:
                importlib.sys.path.append('..')
                xmlhandlers[year] = importlib.import_module(handler)
    return xmlhandlers
