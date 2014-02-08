from lib import alchemy
import pandas as pd

session_generator = alchemy.session_generator
session = session_generator()

#res = session.execute('select rawinventor.name_first, rawinventor.name_last, rawlocation.city, rawlocation.state, \
#                rawlocation.country, rawinventor.sequence, patent.id, \
#                year(application.date), year(patent.date), rawassignee.organization, uspc.mainclass_id, inventor.id \
#                from rawinventor left join patent on patent.id = rawinventor.patent_id \
#                left join application on application.patent_id = patent.id \
#                left join rawlocation on rawlocation.id = rawinventor.rawlocation_id \
#                left join rawassignee on rawassignee.patent_id = patent.id \
#                left join uspc on uspc.patent_id = patent.id \
#                left join inventor on inventor.id = rawinventor.inventor_id \
#                where uspc.sequence = 0;')
res = session.execute('select rawinventor.name_first, rawinventor.name_last, rawlocation.city, rawlocation.state, \
                rawlocation.country, rawinventor.sequence, patent.id, year(application.date), \
                year(patent.date), rawassignee.organization, uspc.mainclass_id, inventor.id \
                from rawinventor, rawlocation, patent, application, rawassignee, uspc, inventor \
                where rawinventor.patent_id = patent.id and \
                application.patent_id = patent.id and \
                rawlocation.id = rawinventor.rawlocation_id and \
                rawassignee.patent_id = patent.id and \
                uspc.patent_id = patent.id and \
                inventor.id = rawinventor.inventor_id;')
data = pd.DataFrame.from_records(res.fetchall())
data = data.drop_duplicates((6,11))
data.columns = ['first_name', 'last_name', 'city', 'state', 'country', 'sequence', 'patent', 'app_year', 'grant_year', 'assignee', 'mainclass', 'inventorid']
data.to_csv('invpat.csv',index=False,encoding='utf8')
