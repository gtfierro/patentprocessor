import os
import glob
import pickle
from ConfigParser import ConfigParser
from IPython.parallel import Client

config = ConfigParser()
config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))

rc = Client(packer="pickle")
dview = rc[:]
print rc.ids


@dview.remote(block=True)
def fetch():
    import os
    os.chdir(node)
    os.system("rm *.xml")
    os.system("rm *.zip")

    for i, f in enumerate(files):
        fname = f.split("/")[-1].split(".")[0]
        os.system("wget {0}".format(f))
        os.system("unzip {0}.zip".format(fname))


fname = open("{0}/urls.pickle".format(config.get('directory', 'sqlalchemy')), "rb")
urls = pickle.load(fname)

master = config.get('directory', 'home')
node = config.get('directory', 'local')
if not os.path.exists("{0}/tar".format(master)):
    os.makedirs("{0}/tar".format(master))

dview["master"] = master
dview["node"] = node
full = []
for year in urls.keys():
    full.extend(urls[year])

files = glob.glob("/home/sgeadmin/patentprocessor/tar/*.tar.gz")
files = [x.split("/")[-1].split(".")[0] for x in files]

fullc = []
for url in full:
    if url.split("/")[-1].split(".")[0] in files:
        fullc.append(url)

dview.scatter("files", fullc)
fetch()

#sudo apt-get install -y python-mysqldb python-pip sqlite3
#sudo pip install unidecode sqlalchemy
