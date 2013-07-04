import os
import pickle
from datetime import datetime
from ConfigParser import ConfigParser
from IPython.parallel import Client

config = ConfigParser()
config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))

rc = Client(packer="pickle")
dview = rc[:]
dview.scatter("ids", rc.ids)
print rc.ids


@dview.remote(block=True)
def fetch(year):
    import glob
    import os
    ids = ids[0]
    os.chdir(node)
    for i, f in enumerate(files):
        fname = f.split("/")[-1].split(".")[0]
        if not os.path.exists("{0}.xml".format(fname)):
            os.system("wget {0}".format(f))
            os.system("unzip {0}.zip".format(fname))
    #         os.chdir(master)
    #         os.system("python parse_sq.py -p {0} --xmlregex {1} >> tar/{2}.log".format(node, fname, ids))
    #         os.system("mysqldump -root uspto -T /var/lib/mysql/uspto")
    #         os.chdir("/var/lib/mysql/uspto")
    #         os.system("tar -czf {0}.tar.gz *.txt".format(fname))
    #         for txts in glob.glob("*.txt"):
    #             os.system("cat {0} >> {0}.full".format(txts))
    #         os.system("rm *.txt")
    #         os.system("scp {0}.tar.gz {1}/tar".format(fname, master))

    # os.chdir("/var/lib/mysql/uspto")
    # os.system("tar -czf {0}.tar.gz *.txt.full".format(ids))
    # os.system("mv {0}.tar.gz {1}/tar".format(ids, master))


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
dview.scatter("files", full)
fetch()

#sudo apt-get install -y python-mysqldb python-pip sqlite3
#sudo pip install unidecode sqlalchemy
