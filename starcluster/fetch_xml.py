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
def makedir(node):
    import os
    if not os.path.exists(node):
        os.makedirs(node)


@dview.remote(block=True)
def fetch(master, node):
    import os
    for f in files:
        fname = f.split("/")[-1]
        os.chdir(master)
        if not os.path.exists("{0}/{1}".format(master, fname)):
            os.system("wget {0}".format(f))


@dview.remote(block=True)
def extract(master, node):
    import os
    for f in files:
        fname = f.split("/")[-1].split(".")[0]
        os.chdir(master)
        if not os.path.exists("{0}/{1}.xml".format(node, fname)):
            os.system("unzip {1} -d {0}.zip".format(node, fname))


fname = open("{0}/urls.pickle".format(config.get('directory', 'sqlalchemy')), "rb")
urls = pickle.load(fname)

print "mkdir"
for year in urls.keys():
    master = "{0}/{1}".format(config.get('directory', 'xml'), year)
    node = "{0}/{1}".format(config.get('directory', 'local'), year)
    if not os.path.exists(master):
        os.makedirs(master)
    makedir(node)

print "wget"
for year in urls.keys():
    print year, datetime.now()
    dview.scatter("files", urls[year])
    master = "{0}/{1}".format(config.get('directory', 'xml'), year)
    node = "{0}/{1}".format(config.get('directory', 'local'), year)
    print "  *", master
    fetch(master, node)

print "extract"
for year in urls.keys():
    print year, datetime.now()
    dview.scatter("files", urls[year])
    master = "{0}/{1}".format(config.get('directory', 'xml'), year)
    node = "{0}".format(config.get('directory', 'local'))
    print "  *", master
    fetch(master, node)

#sudo apt-get install -y python-mysqldb python-pip sqlite3
#sudo pip install unidecode sqlalchemy
