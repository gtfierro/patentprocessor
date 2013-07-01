import ConfigParser

defaults = {'parse': 'defaultparse',
            'clean': True,
            'consolidate': True,
            'datadir': '/data/patentdata/patents/2013',
            'dataregex': 'ipg\d{6}.xml'}

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
    options = {}
    options['datadir'] = handler.get(section,'datadir')
    options['dataregex'] = handler.get(section,'dataregex')
    return options

def get_config_options(configfile):
    handler = ConfigParser(defaults)
    handler.read(configfile)
    process_config = extract_process_options(handler)
    parse_config = extract_parse_options(handler, process_config['parse'])
    return process_config, parse_config
