#!/usr/bin/env python
import sys
from socratapython import socrata
import credentials

print vars(socrata)
# this should really use OAuth 2.0
#reader = socrata.Socrata('https://data.cityofnewyork.us/','PLACE USER ID HERE','PASSWORD HERE','PLACE TOKEN HERE')
reader = socrata.Socrata('https://data.cityofnewyork.us/',credentials.Credentials.user,credentials.Credentials.pass1,credentials.Credentials.token)
print vars(reader)
print reader.setDataset('erm2-nwe9')

##  [{u'school_region': u'Unspecified',
##  u'unique_key': u'27981999',
##  u'street_name': u'CALHOUN AVENUE',
##  u'community_board': u'10 BRONX',
##  u'school_number': u'Unspecified',
##  u'borough': u'BRONX',
##  u'city': u'BRONX',
##  u'incident_zip': u'10465',
##  u'agency': u'DSNY',
##  u'facility_type': u'N/A',
##  u'school_state': u'Unspecified',
##  u'school_phone_number': u'Unspecified',
##  u'location': {u'latitude': u'40.83265921699888',
##  u'needs_recoding': False,
##  u'longitude': u'-73.83023457281958'},
##  u'latitude': u'40.83265921699888',
##  u'address_type': u'ADDRESS',
##  u'status': u'Open',
##  u'school_address': u'Unspecified',
##  u'resolution_action_updated_date': u'2014-05-06T03:05:28',
##  u'x_coordinate_state_plane': u'1031229',
##  u'school_name': u'Unspecified',
##  u'y_coordinate_state_plane': u'242683',
##  u'school_city': u'Unspecified',
##  u'agency_name': u'Department of Sanitation',
##  u'park_borough': u'BRONX',
##  u'school_code': u'Unspecified',
##  u'cross_street_1': u'BRUCKNER BOULEVARD',
##  u'cross_street_2': u'ST RAYMONDS CEMETERY BOUNDARY',
##  u'longitude': u'-73.83023457281958',
##  u'complaint_type': u'Graffiti',
##  u'descriptor': u'Graffiti',
##  u'created_date': u'2014-05-06T03:05:28',
##  u'school_zip': u'Unspecified',
##  u'incident_address': u'1112 CALHOUN AVENUE',
##  u'park_facility_name': u'Unspecified'}]

items=reader.get({'select': 'count(*)'})
print items

items=reader.get({'select':'created_date','order': 'created_date desc','limit':'2'})
print items

items=reader.get({'select':'created_date','order': 'created_date asc','limit':'2'})
print items

sys.exit()

try:
    #items=reader.get({'where': 'created_date >= 2014-05-04', 'limit':'2'})
    #items=reader.get({'order': 'created_date desc', 'limit':'10'})
    #items=reader.get({'order': 'created_date desc', 'limit':'1'})

    items=reader.get({'select': 'count(*)'})
    #items=reader.get({'select': 'count(complaint_type) as count1,complaint_type, year(resolution_action_updated_date) as year1', 'group':'complaint_type,year1', 'order':'count1 desc'})

    #items=reader.get({'limit': '2'})
    #items=reader.get()
except Exception,e:
    print 'e1', e
print items

sys.exit()

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
