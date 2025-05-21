#------------------------------------------------------------------------------
#
# Project:      Bet365 Dezign For Database To Mapping Specification
# Script:       DezignToMapping.py
#
# Purpose:      This python script will take a Dezign For Database and export the ERD to Mapping Specification templates
#
# Version:      v0.0  David Ogden  20 March 2025  First version
#
#------------------------------------------------------------------------------
    
#Requirements:
#          Mapping Specification - Template.xlsx
#          B365_EDW Data Model.dez

import os 
import xml.etree.ElementTree as ET
import datetime
import json
from openpyxl import load_workbook
import shutil

##User Variables
v_MappingSpecificationFileName = 'Mapping Specification - Template.xlsx'
v_DezignFileName = 'B365_EDW Data Model.dez'#'B365_EDW Data Model.dez'

##Mapping Objects
mp_tablename = ''
mp_tableschema = ''
mp_tabledescription = ''
mp_dictAttributes = {}
mp_source_table = ''
mp_source_attribute = ''
mp_sourced_derived = ''
mp_not_null = ''
mp_attribute_name = ''
mp_attribute_description = ''
mp_attribute_datatype = ''
mp_attribute_source_table = ''
mp_attribute_source_attribute = ''
mp_attribute_sourced_derived = ''
mp_attribute_notnull = ''
mp_source_list = []

##Vaviables
v_loop_count = 0
v_version = '-1'
v_diagram = '-1'
v_diagram_id = ''
v_attributeID = ''
v_UDPID = ''
v_UDPType = ''
v_UDPNumber = 0
v_IsPrimaryKey = 0
v_DefaultValues = ''
v_DefaultRecords1 = ''
v_DefaultRecords2 = ''
V_RowsInserted = 0

##Dictionaries
dictPKChild = {}
dictVersions = {}
dictUDP = {}
dictUDPEntityOutput = {}
dictDiagrams = {}
dictEntities = {}
dictObjectRelationships = {}
dictRelationships = {}
dictRelationshipsPK = {}
dictRelationshipsFK = {}
dictAttributes = {}
dictUDPDBList = {}
dictVersionControl = {}
dictDefaultDataTypes = {}

#
#Get current working directory
v_dir_path = os.path.dirname(os.path.realpath(__file__))
v_DezignFileLocation = v_dir_path+'\\'+v_DezignFileName
v_MappingSpecificationLocation = v_dir_path+'\\'+v_MappingSpecificationFileName
#Create a save directory
mydir = os.path.join(os.getcwd(), 'Output') #os.path.join(os.getcwd(), datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
os.makedirs(mydir, exist_ok=True)

#Version Control Details
dictVersionControl[0] = {'Version': 'v0.1', 'Date': datetime.datetime.now().strftime('%d/%m/%Y'), 'Change Reference': '-', 'Description': 'Initial Version - Automated', 'Who By': 'Automated'}
#Add Default Data Types
dictDefaultDataTypes['TIMESTAMP-Start'] = {'Default Values': '"1900-01-01 00:00:00.0000"','Default Records -1': '"1900-01-01 00:00:00.0000"', 'Default Records -2': '"1900-01-01 00:00:00.0000"'}
dictDefaultDataTypes['TIMESTAMP-End'] = {'Default Values': '"9999-12-31 23:59:59.9999"','Default Records -1': '"9999-12-31 23:59:59.9999"', 'Default Records -2': '"9999-12-31 23:59:59.9999"'}
dictDefaultDataTypes['TIMESTAMP'] = {'Default Values': '"1900-01-01 00:00:00.0000"','Default Records -1': '"1900-01-01 00:00:00.0000"', 'Default Records -2': '"9999-12-31 23:59:59.9999"'}
dictDefaultDataTypes['DATETIME'] = {'Default Values': '"1900-01-01 00:00:00.0000"','Default Records -1': '"1900-01-01 00:00:00.0000"', 'Default Records -2': '"9999-12-31 23:59:59.9999"'}
dictDefaultDataTypes['DATE'] = {'Default Values': '"1900-01-01"','Default Records -1': '"1900-01-01"', 'Default Records -2': '"9999-12-31"'}
dictDefaultDataTypes['INT64'] = {'Default Values': '-1','Default Records -1': '-1', 'Default Records -2': '-2'}
dictDefaultDataTypes['STRING'] = {'Default Values': '""','Default Records -1': '""', 'Default Records -2': '""'}
dictDefaultDataTypes['BOOL'] = {'Default Values': 'NULL','Default Records -1': 'NULL', 'Default Records -2': 'NULL'}
dictDefaultDataTypes['NUMERIC'] = {'Default Values': '0','Default Records -1': '0', 'Default Records -2': '0'}
dictDefaultDataTypes['FLOAT64'] = {'Default Values': '"0.0"','Default Records -1': '"0.0"', 'Default Records -2': '"0.0"'}
dictDefaultDataTypes['DEFAULT'] = {'Default Values': '','Default Records -1': '', 'Default Records -2': ''}

tree = ET.parse(v_DezignFileLocation)
root = tree.getroot()


#Allow user to select a version
for versions in root.findall("./VERSION"):
    if(versions.find('VERSIONINFO/VERSIONNUMBER')==None):
        dictVersions['0'] = {'Version': '0', 'Date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Description':'Current'}
    else:
        dictVersions[versions.find('VERSIONINFO/VERSIONNUMBER').text] = {'Version': versions.find('VERSIONINFO/VERSIONNUMBER').text, 'Date':versions.find('VERSIONINFO/VERSIONDATE').text, 'Description':versions.find('VERSIONINFO/VERSIONTYPEDESC').text}
print(json.dumps(dictVersions, indent=2))
while any(v_version in d for d in dictVersions)==False:
    v_version = input("Select Version to document:")
    if(any(v_version in d for d in dictVersions)==False):
        print('Version doesnt exist, try again')


#iterate through all versions and only do analysis for selected version
for versions in root.findall("./VERSION"):
    if (v_version == '0' and versions.find('./VERSIONINFO/VERSIONNUMBER')==None) or (versions.find('./VERSIONINFO/VERSIONNUMBER')!=None and v_version == versions.find('./VERSIONINFO/VERSIONNUMBER').text):
        
        #Get Userdefined dictionary
        for udp in versions.findall("./DATADICT/USERDEFINEDPROPERTIES/USERDEFPROP"):
            if udp.find('NAME').text[0].isdigit():
                v_UDPID = udp.find('ID').text
                v_UDPDBNumber = udp.find('NAME').text[udp.find('NAME').text.rfind("_")-udp.find('NAME').text.find("_")+udp.find('NAME').text.find("_")+1:len(udp.find('NAME').text)]
                v_UDPType = (udp.find('NAME').text[udp.find('NAME').text.find("_")+1:udp.find('NAME').text.rfind("_")-udp.find('NAME').text.find("_")+udp.find('NAME').text.find("_")])
                v_UDPName = (udp.find('NAME').text[udp.find('NAME').text.find("_")+1:])
                v_UDPOrder = udp.find('NAME').text[:udp.find('NAME').text.find("_")]
                dictUDP[v_UDPName] = {'ID': v_UDPID, 'Type': v_UDPType, 'DBNumber': v_UDPDBNumber}
                dictUDPDBList[v_UDPDBNumber] = {'DBNumber': v_UDPDBNumber}


        #Get Diagrams dictionary
        v_loop_count = 0
        for diagrams in versions.findall("./DIAGRAMS/DIAGRAM"):
            v_loop_count = v_loop_count+1
            dictDiagrams[str(v_loop_count)] = {'Name': diagrams.find('NAME').text, 'ID': diagrams.find('ID').text}
        
        #Prompt user to select a diagram
        print(json.dumps(dictDiagrams, indent=2))
        while any(v_diagram in d for d in dictDiagrams)==False:
            v_diagram = input("Select Diagram to document:")
            if(any(v_diagram in d for d in dictDiagrams)==False):
                print('Diagram doesnt exist, try again')
        v_diagram_id = (dictDiagrams[v_diagram]['ID'])
        #Get a list of all diagram objects
        
        #Entities
        for ent in versions.findall("./DIAGRAMS/CONTROLS/ENTITYCONTROLS/ENTC"):
            if (ent.find('DIAGRAMID').text == v_diagram_id):
                dictEntities[ent.find('ID').text] = {'ID': ent.find('ID').text}

        #Get a list of allowed objects based on version and DiagramRelationships
        for ent in versions.findall("./DIAGRAMS/CONTROLS/RELATIONSHIPCONNECTORS/RELC"):
            if (ent.find('DIAGRAMID').text == v_diagram_id):
                dictObjectRelationships[ent.find('ID').text] = {'ID': ent.find('ID').text}
        #List all PK FK combinations in dictRelationships
        for ent in versions.findall("./DATADICT/RELATIONSHIPS/REL"):
            #see if the object exists in the allowed list. If it does then add it to the dictionary, if not the statement fails and the except gets triggered that just passes to the next item.
            try:
                dictObjectRelationships[ent.find('ID').text]
                dictRelationships[ent.find('ID').text] = {'ID': ent.find('ID').text, 'NAME': ent.find('NAME').text, 'PARENTOBJECTID': ent.find('PARENTOBJECTID').text, 'CHILDOBJECTID': ent.find('CHILDOBJECTID').text, 'KEYID': ent.find('./PAIRS/PAIR/KEYID').text, 'FOREIGNKEYID': ent.find('./PAIRS/PAIR/FOREIGNKEYID').text}
                dictRelationshipsPK[ent.find('./PAIRS/PAIR/KEYID').text] = {'ID': ent.find('ID').text, 'NAME': ent.find('NAME').text, 'PARENTOBJECTID': ent.find('PARENTOBJECTID').text, 'CHILDOBJECTID': ent.find('CHILDOBJECTID').text, 'KEYID': ent.find('./PAIRS/PAIR/KEYID').text, 'FOREIGNKEYID': ent.find('./PAIRS/PAIR/FOREIGNKEYID').text}
                dictRelationshipsFK[ent.find('./PAIRS/PAIR/FOREIGNKEYID').text] = {'ID': ent.find('ID').text, 'NAME': ent.find('NAME').text, 'PARENTOBJECTID': ent.find('PARENTOBJECTID').text, 'CHILDOBJECTID': ent.find('CHILDOBJECTID').text, 'KEYID': ent.find('./PAIRS/PAIR/KEYID').text, 'FOREIGNKEYID': ent.find('./PAIRS/PAIR/FOREIGNKEYID').text}
            except Exception as e:
                pass
        #print(dictRelationshipsFK)

        #Get the details about the entities
        for ent_details in dictEntities:
            diag = versions.find(".DATADICT/ENTITIES/ENT/[ID='"+ent_details+"']")
            dictEntities[ent_details] = {'ID': ent_details, 'Name': diag.find('NAME').text}
            mp_tablename = diag.find('NAME').text
            print('Table: ' + str(mp_tablename))
            mp_tableschema = diag.find('SCHEMA').text if diag.find('SCHEMA') is not None else ''
            mp_tabledescription = diag.find('DESC').text if diag.find('DESC') is not None else ''
            
            #Create a copy of the template for this table
            v_WorkingTableFileLocation = mydir+'\\'+mp_tablename+".xlsx"
            shutil.copy(v_MappingSpecificationLocation, v_WorkingTableFileLocation)
            wb = load_workbook(v_MappingSpecificationLocation)
            sheet = wb['Transformation - Sourcing (1)']
            sheet['B3'] = mp_tablename
            sheet['B4'] = mp_tabledescription
            
            sheet = wb['Version Control'] # {'Version': 'v0.1', 'Date': datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), 'Change Reference': '-', 'Description': 'Initial Version - Automated', 'Who By': 'Automated'}
            sheet['B5'] = dictVersionControl[0]['Version']
            sheet['C5'] = dictVersionControl[0]['Date']
            sheet['D5'] = dictVersionControl[0]['Change Reference']
            sheet['E5'] = dictVersionControl[0]['Description']
            sheet['F5'] = dictVersionControl[0]['Who By']
            
            mp_source_list = []
            #Find the entities attributes (Columns)
            mp_dictAttributes = {}
            for attr in diag.findall("./ATTRIBUTES/ATTR"):
                v_attributeID = attr.find('ID').text
                #print(attr.find('ID').text)
                #print(attr.find('NAME').text)
                
                #See if the column is derived or sourced
                mp_not_null = 0
                try:
                    mp_not_null = str(attr.find('./NNCON/VALUE').text)
                except:
                    pass
                mp_not_null = 'Y' if mp_not_null == '1' else 'N'
                mp_sourced_derived = ''
                try:
                    dictRelationshipsFK[attr.find('ID').text]
                    mp_sourced_derived = 'Derived'
                except:
                    mp_sourced_derived = 'Sourced'
                    pass
                #Check to see if column is a primary key
                v_IsPrimaryKey = 0
                try:
                    dictRelationshipsPK[attr.find('ID').text]
                    mp_sourced_derived = 'Derived'
                except:
                    pass
                    
               #Find All UDP for source tables
                dictMiniUDP = {}
                for lp_udp in attr.findall("./USERDEFPROPS"):
                    dictMiniUDP = {}
                    for udp_items in lp_udp:
                        dictMiniUDP[udp_items.tag[4:]] = {'ID': udp_items.tag[4:], 'value': udp_items.text}
                    #print(dictMiniUDP)
                mp_source_table = []
                mp_source_attribute = []
                for key, value in dictUDPDBList.items():
                    try:
                        #print((dictMiniUDP[dictUDP['source_table_'+str(key)]['ID']]['value']))
                        mp_source_table.append(str(dictMiniUDP[dictUDP['source_table_'+str(key)]['ID']]['value']))
                        mp_source_attribute.append(str(dictMiniUDP[dictUDP['source_column_'+str(key)]['ID']]['value']))
                        mp_source_list.append(str(dictMiniUDP[dictUDP['source_table_'+str(key)]['ID']]['value']))
                    except Exception as e:
                        #print(e)
                        pass
                #Check to see if column needs to be overridden
                
                mp_attribute_name = attr.find('NAME').text
                mp_attribute_description = attr.find('DESC').text if attr.find('DESC') is not None else ''
                mp_attribute_datatype = attr.find('DT/DTLISTNAME').text
                mp_attribute_source_table = ', \n'.join(mp_source_table)
                mp_attribute_source_attribute = ', \n'.join(mp_source_attribute)
                mp_attribute_sourced_derived = mp_sourced_derived
                mp_attribute_notnull = mp_not_null
                
                #Set the default values 
                if mp_attribute_datatype == 'TIMESTAMP' and mp_attribute_name == 'effective_start_utc_timestamp' :
                    v_DefaultValues = dictDefaultDataTypes['TIMESTAMP-Start']['Default Values']
                    v_DefaultRecords1 = dictDefaultDataTypes['TIMESTAMP-Start']['Default Records -1']
                    v_DefaultRecords2 = dictDefaultDataTypes['TIMESTAMP-Start']['Default Records -2']
                elif mp_attribute_datatype == 'TIMESTAMP' and mp_attribute_name == 'effective_end_utc_timestamp' :
                    v_DefaultValues = dictDefaultDataTypes['TIMESTAMP-End']['Default Values']
                    v_DefaultRecords1 = dictDefaultDataTypes['TIMESTAMP-End']['Default Records -1']
                    v_DefaultRecords2 = dictDefaultDataTypes['TIMESTAMP-End']['Default Records -2']
                else:
                    try:
                        v_DefaultValues = dictDefaultDataTypes[mp_attribute_datatype]['Default Values']
                        v_DefaultRecords1 = dictDefaultDataTypes[mp_attribute_datatype]['Default Records -1']
                        v_DefaultRecords2 = dictDefaultDataTypes[mp_attribute_datatype]['Default Records -2']
                    except:
                        v_DefaultValues = dictDefaultDataTypes['DEFAULT']['Default Values']
                        v_DefaultRecords1 = dictDefaultDataTypes['DEFAULT']['Default Records -1']
                        v_DefaultRecords2 = dictDefaultDataTypes['DEFAULT']['Default Records -2']
                        pass
                
                #Write data to dictionary 
                mp_dictAttributes[attr.find('ID').text] = {'Attribute_Name': mp_attribute_name, 'Attribute_Description': mp_attribute_description, 'Datatype': mp_attribute_datatype, 'source_table': mp_attribute_source_table, 'source_attribute': mp_attribute_source_attribute, 'sourced_derived': mp_attribute_sourced_derived, 'NotNull' : mp_attribute_notnull, 'Default Values': v_DefaultValues, 'Default Records -1': v_DefaultRecords1, 'Default Records -2': v_DefaultRecords2}
            

            
            #Write results to excel
            sheet = wb['Transformation - Sourcing (1)']
            #Sourcing section
            V_RowsInserted = 0
            mp_source_list = list(dict.fromkeys(mp_source_list))
            for i in range(len(mp_source_list)):
                if i >= 9:
                    sheet.insert_rows(21)
                    V_RowsInserted = V_RowsInserted+1
                sheet [f"C{i+11}"] = mp_source_list[i]
            #Transformation section
            for row, (key, data) in enumerate(mp_dictAttributes.items(), start=2):
                sheet [f"B{row+28+V_RowsInserted}"] = data['Attribute_Name']
                sheet [f"C{row+28+V_RowsInserted}"] = data['Attribute_Description']
                sheet [f"D{row+28+V_RowsInserted}"] = data['Datatype']
                sheet [f"F{row+28+V_RowsInserted}"] = data['sourced_derived']
                if data['sourced_derived'] == 'Sourced':
                    sheet [f"G{row+28+V_RowsInserted}"] = data['source_table']
                    sheet [f"H{row+28+V_RowsInserted}"] = data['source_attribute']
                else:
                    sheet [f"J{row+28+V_RowsInserted}"] = data['source_table']
                    sheet [f"I{row+28+V_RowsInserted}"] = data['source_attribute']
                sheet [f"L{row+28+V_RowsInserted}"] = data['NotNull']
                sheet [f"M{row+28+V_RowsInserted}"] = data['Default Values']
                sheet [f"N{row+28+V_RowsInserted}"] = data['Default Records -1']
                sheet [f"O{row+28+V_RowsInserted}"] = data['Default Records -2']
            
            #Write results to file
            wb.save(v_WorkingTableFileLocation) 
