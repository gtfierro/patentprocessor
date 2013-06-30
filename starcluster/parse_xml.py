import os
import sys
sys.path.append("..")
import parse_sq
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
        params['commit'] = 100

    f = open("status", "ab")
    os.chdir(params['patentroot'])
    for xml in glob("*.xml"):
        params["xmlregex"] = xml
        try:
            parse_sq.main(**params)
        except Exception as inst:
            print xml, inst
            f.write("{} {}\n".format(xml, inst))
