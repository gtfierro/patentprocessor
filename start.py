import re
import sys
import parse
import time
from IPython.parallel import Client

sys.path.append('lib')
from config_parser import get_config_options

def connect_client():
    """
    Loops for a minute until the Client connects to the local ipcluster
    """
    start=time.time()
    print 'Client connecting...'
    while time.time()-start < 60:
        try:
            c = Client()
            dview = c[:]
            break
        except:
            time.sleep(5)
            continue
    return dview

# accepts path to configuration file as command line option
process_config, parse_config = get_config_options(sys.argv[1])
files = parse.list_files(parse_config['datadir'],parse_config['dataregex'])
print files
dview = connect_client()
dview.block=True
dview.scatter('files',files)
dview['process_config'] = process_config
dview['parse_config'] = parse_config

def run_process():
    import parse
    import time
    parsed_xmls = parse.parallel_parse(files)
    parsed_grants = parse.parse_patent(parsed_xmls)
    parse.build_tables(parsed_grants)
#TODO: gather from cores
#parse.commit_tables()
parse.move_tables(process_config['outputdir'])

print dview.apply(run_process)
