import os
import pickle
from datetime import datetime
from ConfigParser import ConfigParser
from IPython.parallel import Client

config = ConfigParser()
config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))

rc = Client(packer="pickle")
dview = rc[:]
print rc.ids


@dview.remote(block=True)
def fetch(directory):
    import os
    if not os.path.exists(directory):
        os.makedirs(directory)
    for f in files:
        os.chdir(directory)
        os.system("wget {0}".format(f))
        f = f.split("/")[-1]
        os.system("unzip {0}".format(f))


url = pickle.loads(open("{}/urls.pickle".format(config.get('directory', 'sqlalchemy'), "rb")

for year in xrange(2005, 2014):
    print year, datetime.now()
    dview.scatter("files", urls[year])
    directory = "{}/{}".format(config.get('directory', 'storage'), year)
    fetch(directory)
