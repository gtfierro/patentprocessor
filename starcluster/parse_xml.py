import os
import sys
sys.path.append("/home/sgeadmin/patentprocessor")
import parse_sq
import pickle
from glob import glob
from ConfigParser import ConfigParser

config = ConfigParser()
config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))


if __name__ == '__main__':
    params = {}
    if len(sys.argv) >= 2:
        params['patentroot'] = sys.argv[1]
    else:
        params['patentroot'] = config.get('directory', 'local')

    if len(sys.argv) >= 3:
        params['commit'] = sys.argv[2]
    else:
        params['commit'] = 1

    if os.path.exists("/mnt/sgeadmin/loaded.pickle"):
        loaded = pickle.load(open("/mnt/sgeadmin/loaded.pickle", "rb"))
    else:
        loaded = []

    f = open("/mnt/sgeadmin/status", "ab")
    os.chdir(params['patentroot'])

    for xml in glob("*.xml"):
        if xml not in loaded:
            params["xmlregex"] = xml
            try:
                parse_sq.main(**params)
                loaded.append(xml)
                pickle.dump(loaded, open("/mnt/sgeadmin/loaded.pickle", "wb"))
            except Exception as inst:
                print xml, inst
                f.write("{} {}\n".format(xml, inst))
