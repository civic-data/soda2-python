#!/usr/bin/env python
# boro,block,lot,bin,lhnd,lhns,lcontpar,lsos,hhnd,hhns,hcontpar,hsos,scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,segid,zipcode
import sys
import csv
import re
from datetime import datetime

csvwriter = csv.writer(sys.stdout, delimiter=',', quotechar='"')
datesearch = re.compile('\d\d/\d\d/\d\d\d\d')
numbersearch = re.compile('^\d')
numbersearch2 = re.compile('.*\d$')
reader = csv.DictReader( sys.stdin )
headersave=[]

# bq --project personal-real-estate load nyc.311 ~/download2/311_Service_Requests_from_2010_to_Present.csv  Unique_Key:string,Created_Date:timestamp,Closed_Date:timestamp,Agency:string,Agency_Name:string,Complaint_Type:string,Descriptor:string,Location_Type:string,Incident_Zip:string,Incident_Address:string,Street_Name:string,Cross_Street_1:string,Cross_Street_2:string,Intersection_Street_1:string,Intersection_Street_2:string,Address_Type:string,City:string,Landmark:string,Facility_Type:string,Status:string,Due_Date:timestamp,Resolution_Action_Updated_Date:timestamp,Community_Board:string,Borough:string,X_Coordinate_State_Plane:string,Y_Coordinate_State_Plane:string,Park_Facility_Name:string,Park_Borough:string,School_Name:string,School_Number:string,School_Region:string,School_Code:string,School_Phone_Number:string,School_Address:string,School_City:string,School_State:string,School_Zip:string,School_Not_Found:string,School_or_Citywide_Complaint:string,Vehicle_Type:string,Taxi_Company_Borough:string,Taxi_Pick_Up_Location:string,Bridge_Highway_Name:string,Bridge_Highway_Direction:string,Road_Ramp:string,Bridge_Highway_Segment:string,Garage_Lot_Name:string,Ferry_Direction:string,Ferry_Terminal_Name:string,Latitude:string,Longitude:string,Location:string

typedict = { 'Unique_Key':'string','Created_Date':'timestamp','Closed_Date':'timestamp','Agency':'string','Agency_Name':'string','Complaint_Type':'string','Descriptor':'string','Location_Type':'string','Incident_Zip':'string','Incident_Address':'string','Street_Name':'string','Cross_Street_1':'string','Cross_Street_2':'string','Intersection_Street_1':'string','Intersection_Street_2':'string','Address_Type':'string','City':'string','Landmark':'string','Facility_Type':'string','Status':'string','Due_Date':'timestamp','Resolution_Action_Updated_Date':'timestamp','Community_Board':'string','Borough':'string','X_Coordinate_State_Plane':'string','Y_Coordinate_State_Plane':'string','Park_Facility_Name':'string','Park_Borough':'string','School_Name':'string','School_Number':'string','School_Region':'string','School_Code':'string','School_Phone_Number':'string','School_Address':'string','School_City':'string','School_State':'string','School_Zip':'string','School_Not_Found':'string','School_or_Citywide_Complaint':'string','Vehicle_Type':'string','Taxi_Company_Borough':'string','Taxi_Pick_Up_Location':'string','Bridge_Highway_Name':'string','Bridge_Highway_Direction':'string','Road_Ramp':'string','Bridge_Highway_Segment':'string','Garage_Lot_Name':'string','Ferry_Direction':'string','Ferry_Terminal_Name':'string','Latitude':'string','Longitude':'string','Location':'string' }

# typedict={'EX_COUNT':'float','CUREXT_A':'float','TN_EXL':'float','DELCHG':'string','BORO':'integer','EXT':'string','TN_EXT':'float','NOAV':'integer','FN_EXT_A':'float','FN_EXL_A':'float','APPLIC2':'string','CUREXL_A':'float','CONDO_S3':'string','CONDO_S2':'string','CONDO_S1':'string','YRB_FLAG':'string','PROTEST':'string','TN_AVL_A':'float','AP_TIME':'float','COMINT_B':'string','ZIP':'float','NEW_FV_L':'float','NEWLOT':'integer','BLDGS':'string','COMINT_L':'string','NEW_FV_T':'float','O_AT_GRP':'float','NODESC':'integer','YRA2_RNG':'float','COOP_NUM':'float','BLD_VAR':'string','REUC':'string','YRA2':'float','YRA1':'float','DROPLOT':'integer','LIMIT':'string','CP_DIST':'string','CUR_FV_L':'float','TXCL':'string','CURAVL':'float','L_ACRE':'string','O_LIMIT':'string','CUR_FV_T':'float','BLOCK':'integer','CURAVT':'float','DCHGDT':'timestamp','TN_EXT_A':'float','AP_BLOCK':'float','APPLIC':'string','CORCHG':'integer','HNUM_LO':'string','AT_GRP':'float','CHGDT':'timestamp','TN_EXL_A':'float','APTNO':'string','EX_CHGDT':'timestamp','GEO_RC':'integer','FCHGDT':'timestamp','STATUS1':'integer','DISTRICT':'string','STATUS2':'integer','TN_AVT_A':'float','FN_AVT':'float','VALREF':'string','CONDO_NM':'float','GR_SQFT':'float','CUREXT':'float','O_TXCL':'string','CORNER':'string','RES_UNIT':'float','CUREXL':'float','EASE':'string','CP_BORO':'string','YEAR4':'float','FN_AVL_A':'float','CONDO_A':'string','BLDGCL':'string','MBLDG':'string','STR_NAME':'string','YRB':'float','FV_CHGDT':'timestamp','FN_EXT':'float','AP_LOT':'float','BFRT_DEC':'float','AT_GRP2':'float','FN_EXL':'float','IRREG':'string','TN_AVT':'float','AP_BORO':'string','FN_AVL':'float','LND_AREA':'float','O_PROTST':'string','OWNER':'string','YRB_RNG':'float','SM_CHGDT':'timestamp','BBLE':'string','SECVOL':'float','BDEP_DEC':'float','TN_AVL':'float','O_APPLIC':'string','LOT':'integer','CURAVL_A':'float','LDEP_DEC':'float','YRA1_RNG':'float','CURAVT_A':'float','PROTEST2':'string','FN_AVT_A':'float','STORY':'float','ZONING':'string','HNUM_HI':'string','EX_INDS':'string','LFRT_DEC':'float','CBN_TXCL':'string','AP_DATE':'timestamp','TOT_UNIT':'float','EXMTCL':'string','AP_EASE':'string'}
lineno =0
for line in reader:
    lineno = lineno + 1
    row = []
    header=[]
    for key in line:
        try:
# bq load --project personal-real-estate acris_nyc.nyc_dof_tc_Tentative_Assessment_Roll test.small.csv EX_COUNT:float,CUREXT_A:float,TN_EXL:float,DELCHG:string,BORO:integer,EXT:string,TN_EXT:float,NOAV:integer,FN_EXT_A:float,FN_EXL_A:float,APPLIC2:string,CUREXL_A:float,CONDO_S3:string,CONDO_S2:string,CONDO_S1:string,YRB_FLAG:string,PROTEST:string,TN_AVL_A:float,AP_TIME:float,COMINT_B:string,ZIP:float,NEW_FV_L:float,NEWLOT:integer,BLDGS:string,COMINT_L:string,NEW_FV_T:float,O_AT_GRP:float,NODESC:integer,YRA2_RNG:float,COOP_NUM:float,BLD_VAR:string,REUC:string,YRA2:float,YRA1:float,DROPLOT:integer,LIMIT:string,CP_DIST:string,CUR_FV_L:float,TXCL:string,CURAVL:float,L_ACRE:string,O_LIMIT:string,CUR_FV_T:float,BLOCK:integer,CURAVT:float,DCHGDT:timestamp,TN_EXT_A:float,AP_BLOCK:float,APPLIC:string,CORCHG:integer,HNUM_LO:string,AT_GRP:float,CHGDT:timestamp,TN_EXL_A:float,APTNO:string,EX_CHGDT:timestamp,GEO_RC:integer,FCHGDT:timestamp,STATUS1:integer,DISTRICT:string,STATUS2:integer,TN_AVT_A:float,FN_AVT:float,VALREF:string,CONDO_NM:float,GR_SQFT:float,CUREXT:float,O_TXCL:string,CORNER:string,RES_UNIT:float,CUREXL:float,EASE:string,CP_BORO:string,YEAR4:float,FN_AVL_A:float,CONDO_A:string,BLDGCL:string,MBLDG:string,STR_NAME:string,YRB:float,FV_CHGDT:timestamp,FN_EXT:float,AP_LOT:float,BFRT_DEC:float,AT_GRP2:float,FN_EXL:float,IRREG:string,TN_AVT:float,AP_BORO:string,FN_AVL:float,LND_AREA:float,O_PROTST:string,OWNER:string,YRB_RNG:float,SM_CHGDT:timestamp,BBLE:string,SECVOL:float,BDEP_DEC:float,TN_AVL:float,O_APPLIC:string,LOT:integer,CURAVL_A:float,LDEP_DEC:float,YRA1_RNG:float,CURAVT_A:float,PROTEST2:string,FN_AVT_A:float,STORY:float,ZONING:string,HNUM_HI:string,EX_INDS:string,LFRT_DEC:float,CBN_TXCL:string,AP_DATE:timestamp,TOT_UNIT:float,EXMTCL:string,AP_EASE:string
# 
# 
# Unique_Key,Created_Date,Closed_Date,Agency,Agency_Name,Complaint_Type,Descriptor,Location_Type,Incident_Zip,Incident_Address,Street_Name,Cross_Street_1,Cross_Street_2,Intersection_Street_1,Intersection_Street_2,Address_Type,City,Landmark,Facility_Type,Status,Due_Date,Resolution_Action_Updated_Date,Community_Board,Borough,X_Coordinate_State_Plane,Y_Coordinate_State_Plane,Park_Facility_Name,Park_Borough,School_Name,School_Number,School_Region,School_Code,School_Phone_Number,School_Address,School_City,School_State,School_Zip,School_Not_Found,School_or_Citywide_Complaint,Vehicle_Type,Taxi_Company_Borough,Taxi_Pick_Up_Location,Bridge_Highway_Name,Bridge_Highway_Direction,Road_Ramp,Bridge_Highway_Segment,Garage_Lot_Name,Ferry_Direction,Ferry_Terminal_Name,Latitude,Longitude,Location
# 27981999,05/06/2014 03:05:28 AM,,DSNY,Department of Sanitation,Graffiti,Graffiti,,10465,1112 CALHOUN AVENUE,CALHOUN AVENUE,BRUCKNER BOULEVARD,ST RAYMONDS CEMETERY BOUNDARY,,,ADDRESS,BRONX,,N/A,Open,,05/06/2014 03:05:28 AM,10 BRONX,BRONX,1031229,242683,Unspecified,BRONX,Unspecified,Unspecified,Unspecified,Unspecified,Unspecified,Unspecified,Unspecified,Unspecified,Unspecified,,,,,,,,,,,,,40.83265921699888,-73.83023457281958,"(40.83265921699888, -73.83023457281958)"
                  
# 05/06/2014 03:05:28 AM
            if typedict[key]=='timestamp':
                if line[key]=='' or line[key]=='Unspecified':
                    row.append('')
                else:
                    try:
                        date = datetime.strptime(line[key],'%m/%d/%Y %I:%M:%S %p')
                        row.append(datetime.strftime(date,'%Y-%m-%dT%H:%M:%S-05:00'))
                    except Exception,e:
                        print 'issue1:',e, key, line[key]
                        row.append('')
            elif typedict[key]=='integer':
                    row.append(int(float(line[key])))
            elif typedict[key]=='float':
                    row.append(float(line[key]))
            elif typedict[key]=='string':
                    row.append(str(line[key]))



#              if re.match(datesearch,line[key]):
#                  #print 'timestamp',key,line[key]
#                  header.append('%s:timestamp' % key)
#                  try:
#                      date = datetime.strptime(line[key],'%m/%d/%Y')
#                      row.append(datetime.strftime(date,'%Y-%m-%dT00:00:00-05:00'))
#                  except Exception,e:
#                      #print 'issue1:',e
#                      row.append('')
#              elif re.match(numbersearch,line[key]) and re.match(numbersearch2,line[key]):
#                  if line[key].find('.') == -1:
#                      header.append('%s:integer' % key)
#                      row.append(int(line[key]))
#                  else:
#                      header.append('%s:float' % key)
#                      #print 'XXXfloat',key,float(line[key])
#                      try:
#                          row.append(float(line[key]))
#                      except Exception,e:
#                          #print 'issue2:',e,line[key]
#                          #print 'QQQ: ', row
#                          row.append('')
#  #             elif line[key] == '':
#  #                 header.append('blank')
#  #                 row.append(line[key])
#              else:
#                  header.append('%s:string' % key)
#                  #print 'other',key,line[key]
#                  row.append(line[key])
        except Exception,e:
            try:
                sys.stderr.write( 'BIG ISSUE:%s\n'% e)
                #sys.stderr.write( 'BIG ISSUE:%s:%s>>>%s<<< %s\n'% (typedict[key],key,line[key], line))
            except Exception,e:
                #sys.stderr.write( 'REALLY BIG ISSUE:%s:%s\n'% (key,line))
                pass



    #row = [line['BBLE']]
    #print "QQQ",len(row)
    if len(row) == 52:
        csvwriter.writerow(row)
    else:
        #row=['ISSUE']+row
        #csvwriter.writerow(row)
        sys.stderr.write("row count issue %s %s\n" % ( len(row), lineno))
    #print len(row)
    if header!=headersave:
        #print 'HEADER ISSUE',header
        #print 'HEADER ISSUE',headersave
        pass
    headersave=header
