import re
import sys
import parse
import time
import itertools
from IPython.parallel import Client

sys.path.append('lib')
from patSQL import *
from config_parser import get_config_options

assignee_table = AssigneeSQL()
citation_table = CitationSQL()
class_table = ClassSQL()
inventor_table = InventorSQL()
patent_table = PatentSQL()
patdesc_table = PatdescSQL()
lawyer_table = LawyerSQL()
sciref_table = ScirefSQL()
usreldoc_table = UsreldocSQL()

xmlclasses = [AssigneeXML, CitationXML, ClassXML, InventorXML, \
              PatentXML, PatdescXML, LawyerXML, ScirefXML, UsreldocXML]

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
dview = connect_client()
dview.block=True
dview.scatter('files',files)
dview['process_config'] = process_config
dview['parse_config'] = parse_config

def run_process():
    import parse
    import time
    import sys
    import itertools
    parsed_xmls = parse.parallel_parse(files)
    parsed_grants = parse.parse_patent(parsed_xmls)
    parse.build_tables(parsed_grants)
    parse.commit_tables()

parse.move_tables(process_config['outputdir'])

#parsed_grants = itertools.chain.from_iterable(dview.apply(run_process))
parsed_grants = dview.apply(run_process)
print list(parsed_grants)
