from socratapython import socrata
import credentials

print vars(socrata)
# this should really use OAuth 2.0
#reader = socrata.Socrata('https://data.cityofnewyork.us/','PLACE USER ID HERE','PASSWORD HERE','PLACE TOKEN HERE')
reader = socrata.Socrata('https://data.cityofnewyork.us/',credentials.Credentials.user,credentials.Credentials.pass1,credentials.Credentials.token)
print vars(reader)
print reader.setDataset('erm2-nwe9')

try:
    #items=reader.get({'where': 'created_date >= 2014-05-04', 'limit':'2'})
    #items=reader.get({'order': 'created_date desc', 'limit':'10'})
    items=reader.get({'select': 'count(complaint_type) as count1,complaint_type', 'group':'complaint_type', 'order':'count1 desc'})
    #items=reader.get({'limit': '2'})
    #items=reader.get()
except Exception,e:
    print 'e1', e
print items

try:
    items=reader.get({'order': 'created_date desc', 'limit':'1000'})
except Exception,e:
    print 'e1', e

try:
    for item in items:
        #print item
        print '%s %s %s %s' % (item['created_date'], item['complaint_type'],item['latitude'],item['longitude'])
except Exception,e:
    print 'e2', e
