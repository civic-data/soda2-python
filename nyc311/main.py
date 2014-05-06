from socratapython import socrata

print vars(socrata)
# this should really use OAuth 2.0
reader = socrata.Socrata('https://data.cityofnewyork.us/','PLACE USER ID HERE','PASSWORD HERE','xC8nzr6tI03SZuDEPL1tx9WDW')
print vars(reader)
print reader.setDataset('erm2-nwe9')
items=reader.get()
#print items
for item in items:
    print '%s %s' % (item['created_date'], item['complaint_type'])
