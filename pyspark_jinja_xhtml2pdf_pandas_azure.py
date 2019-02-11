# Databricks notebook source
import sys

sys.version

# COMMAND ----------

#import xlrd
#import unicodecsv
import numpy as np
import pandas as pd
from io import BytesIO

# print pandas version
pd.__version__

storage_account_name = "<STORAGE_ACCOUNT_NAME>"
storage_account_access_key = "<STORAGE_ACCOUNT_KEY>"

file_location_products = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/<FILE_NAME>"
file_location_services = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/<FILE_NAME>"
file_location_product_description = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/<FILE_NAME>"
file_location_product_description_fixed = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/<FILE_NAME>"
file_location_mappings = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/<FILE_NAME>"
file_location_systems = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/<FILE_NAME>"

file_type = "csv"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# read all 4 tables here: df_name = df_name.csv
prods_spark_df = spark.read.format(file_type).option("inferSchema", "true").load(file_location_products)
servs_spark_df = spark.read.format(file_type).option("inferSchema", "true").load(file_location_services)
systs_spark_df = spark.read.format(file_type).option("inferSchema", "true").load(file_location_systems)
maps_spark_df = spark.read.format(file_type).option("inferSchema","true").load(file_location_mappings)

product_desc_var = spark.read.format(file_type).option("inferSchema", "true").load(file_location_product_description)
product_desc_fixed = spark.read.format(file_type).option("inferSchema", "true").load(file_location_product_description_fixed)

product_desc_var = product_desc_var.drop('_c6')

product_desc = product_desc_fixed.union(product_desc_var)



display(systs_spark_df)


# COMMAND ----------

display(product_desc_var)

# COMMAND ----------

display(product_desc_fixed)

# COMMAND ----------

systems = systs_spark_df.selectExpr("_c0 as Type"," _c1 as Exists_in_mapping","_c2 as Exists_in_Tool","_c3 as System", "_c4 as Description",	"_c5 as System2", "_c6 as Description2", "_c7 as Address","_c8 as Bank_Details", "_c9 as Biometric_data", "_c10 as criminal_convictions_offences","_c11 as DOB","_c12 as email","_c13 as GeneticData", "_c14 as Health", "_c15 as LocationData", "_c16 as MaidenName", "_c17 as Name", "_c18 as NI_Number","_c19 as Online_Identifier", "_c20 as Phone_Number","_c22 as Political_Opinion", "_c23 as Postcode_area_code","_c24 as Postcode_full", "_c25 as Ethnic_Origin", "_c26 as Religious_Beliefs",	"_c27 as Gender", "_c28 as Trade_Union_Membership", "_c29 as Salary", "_c30 as Spouse_Details","_c31 as HMRC_Data","_c32 as Pension_Details_History", "_c33 as Scanned_documents", "_c34 as Marital_Status", "_c35 as Employment_History", "_c36 as Absence_details","_c37 as Wish_Details")

product_description = product_desc.selectExpr("_c0 as product_name", "_c1 as IAR_description", "_c2 as practice_area" ,"_c3 as type" , "_c4 as parent_product", "_c5 as is_Product_active")

products = prods_spark_df.selectExpr("_c0 as CaseSafeID", "_c1 as Account_Name","_c2 as CEB_Account_Relationship","_c3 as Practice_Area","_c4 as Service_Name","_c5 as Service_Product_Name","_c5 as Account_Owner_Name", "_c6 as email")

services = servs_spark_df.selectExpr("_c0 as Account_Name","_c1 as CaseSafeID","_c2 as Key_Account_Contact_Name","_c3 as KeyAccount_Contact_Last_Name","_c4 as Key_Contact_Job_Title","_c5 as Key_Contact_email","_c6 as Account_Owner_Full_Name","_c7 as Practice_Area","_c8 as Account_Owner_Department_Sub_Team","_c9 as Service_Name","_c10 as Practice_Area2","_c11 as Owner_Email","_c12 as Data_Protection_Officer_Full_Name","_c13 as Data_Protection_OfficerEmail","_c14 as Legal_Basis_for_Processing","_c15 as Confirmed_No_DPO","_c16 as DPO_TBC","_c17 as Scheme_Actuary"
)

mappings = maps_spark_df.selectExpr("_c0 as Client", "_c1 as PracticeArea", "_c2 as Type", "_c3 as Parent_Product", "_c4 as ProductName", "_c5 as MappedSystem", "_c6 as SFID")

display(systems)


# COMMAND ----------


# using spark data frame instead of pandas data frames
# assert(products.select("Account_Name").distinct().count() == products.select("CaseSafeID").distinct().count())

panda_products = products.toPandas()
panda_product_description = product_description.toPandas()
panda_products = panda_products.merge(panda_product_description, left_on = 'Service_Product_Name', right_on = 'product_name', how='left')
panda_mappings = mappings.toPandas()

assert len(panda_products['Account_Name'].unique()) == len(panda_products['CaseSafeID'].unique())
accounts = panda_products[['Account_Name', 'CaseSafeID']].drop_duplicates()
assert len(accounts['Account_Name'].unique()) == len(accounts['CaseSafeID'].unique())
accounts_map = accounts.set_index('CaseSafeID', drop=True).iloc[:,0]

keys_map = {v:k for k,v in accounts_map.iteritems()}

# COMMAND ----------

panda_mappings['Name_from_Products'] = panda_mappings['SFID'].map(accounts_map)

panda_mappings['Name_Check'] = panda_mappings.apply(lambda x: x['Client'] == x['Name_from_Products'] if type(x['SFID']) == str else 'not identified', axis=1)

# apply any name changes to 'Client' column

#if len(panda_mappings.loc[panda_mappings['Name_Check'] == False]) > 0:
#    panda_mappings['Client'] = panda_mappings.apply(lambda x: x['Name_from_Products'] if x['Name_Check'] == False else x, axis = 1)

# check for Product System mapping no longer active

# check and remove
panda_mappings['Concatenation'] = panda_mappings.apply(lambda x: str(x['Client']) + str(x['Parent_Product']) + str(x['ProductName']), axis=1)
panda_products['Concatenation'] = panda_products.apply(lambda x: str(x['Account_Name']) + str(x['Service_Name']) + str(x['Service_Product_Name']), axis=1)

maps_concat = set(panda_mappings['Concatenation'])

# COMMAND ----------

panda_systems = systems.toPandas()

end = panda_systems.shape[1]
lst = [i for i in range(7, end)]
lst.append(5)
lst.sort()

syst = panda_systems.iloc[:, lst]
syst.set_index('System2', drop=True, inplace=True)

syst_desc = panda_systems[['System', 'Description']].set_index('System', drop=True)

# COMMAND ----------

data_dict = {}
found = [0,0]
not_found = []

for i, row in panda_mappings.iterrows():
    try:
        s = row['MappedSystem']
        data_dict[s] = [c for (c,d) in syst.loc[s].iteritems() if d == 'Yes']
        found[0] += 1
    except: 
        pass
#        found[1] += 1
#        not_found.append(row['Parent Product'] + ' - ' + row['Product Name'])
#print (found, not_found)

# COMMAND ----------

def concat_name(first_name, last_name, join_string = ' '):
    if type(first_name) == str:
        if type(last_name) == str:
            full_name = first_name + join_string + last_name
        else:
            full_name = first_name
    else:
        if type(last_name) == str:
            full_name = last_name
        else:
            full_name = None
    return full_name

# COMMAND ----------

panda_services = services.toPandas()
#panda_product_description = product_description.toPandas()

acct_info_all = panda_services[["Account_Name","CaseSafeID","Key_Account_Contact_Name","KeyAccount_Contact_Last_Name","Key_Contact_Job_Title","Key_Contact_email","Account_Owner_Full_Name","Practice_Area","Account_Owner_Department_Sub_Team","Service_Name","Practice_Area2","Owner_Email","Data_Protection_Officer_Full_Name","Data_Protection_OfficerEmail","Legal_Basis_for_Processing","Confirmed_No_DPO","DPO_TBC","Scheme_Actuary"]].drop_duplicates()

acct_info_all['Key_Account_Contact_Full_Name'] = concat_name(acct_info_all['Key_Account_Contact_Name'], 
                                                                  acct_info_all['KeyAccount_Contact_Last_Name'])

# COMMAND ----------

# functions to return required client data

class Client:
    
    def __init__(self, client_name):
        self.name = client_name
    
        # select client-specific data
    
        self.prods = panda_products.loc[panda_products['Account_Name'] == self.name]
        #self.prods = self.prods.merge(panda_product_description, left_on = 'Service_Product_Name', right_on = 'product_name', how='left')
        self.prod_syst = panda_mappings.loc[panda_mappings['Client'] == self.name]

        self.serv = panda_services.loc[panda_services['Account_Name'] == self.name]
        self.list_services = self.serv['Service_Name']

        self.ls = [i for i in self.list_services.unique()]
        
        self.long_acct_info = acct_info_all.loc[acct_info_all['Account_Name'] == self.name]
        
        
        # if client appears in services table
        
        if self.long_acct_info.shape[0] > 0:
            self.acct_info = self.long_acct_info.iloc[0,:]
            
            self.gen_info = {'Account_Name_Data_Controller' : self.name,
                             'CES_Reference' : keys_map[self.name],
                             'Account_Owner_cpt' : self.acct_info['Account_Owner_Full_Name'],
                             'Account_Owner_cpt_Email': self.acct_info['Owner_Email'],
                             'Scheme_Actuary' : self.acct_info['Scheme_Actuary'],
                             'CES_Actuarial_Legal_Basis_for_Processing' : self.acct_info['Legal_Basis_for_Processing'],
                             'Key_Contact_Name' : self.acct_info['Key_Account_Contact_Full_Name'],
                             'Key_Contact_Title' : self.acct_info['Key_Contact_Job_Title'],
                             'Key_Contact_Email' :  self.acct_info['Key_Contact_email'],
                             'Data_Protection_Officer_Name' : self.acct_info['Data_Protection_Officer_Full_Name'],
                             'Data_Protection_Officer_Email': self.acct_info['Data_Protection_OfficerEmail'],
                             'Data_Controller_Legal_Basis_for_Processing': self.acct_info['Legal_Basis_for_Processing']
                            }
        # if client only appears in the products table
        
        else:                             
            self.acct_info = panda_products.loc[panda_products['Account_Name'] == self.name].iloc[0,:]

            self.gen_info = {'Account Name / Data Controller' : self.name,
                             'CES Reference' : keys_map[self.name],
                             'Account Owner (cpt)' : self.acct_info['Account_Owner_Name'],
                             'Account Owner (cpt) Email': None,
                             'Scheme Actuary' : None,
                             'CES Actuarial Legal Basis for Processing' : None,
                             'Key Contact Name' : None,
                             'Key Contact Title' : None,
                             'Key Contact Email' :  self.acct_info['email'],
                             'Data Protection Officer Name' : None,
                             'Data Protection Officer Email': None,
                             'Data Controller Legal Basis for Processing': None
                            }
       
        
    def section_3_headings(self):
        return {sec : name for sec, name in enumerate(self.ls, 1)}
        
    def services_data(self):

        service_desc_dict = {}
        for i in self.ls:
            section_i = []

            for idx, row in self.prods.loc[self.prods['Service_Name'] == i].iterrows():
                serv_desc = concat_name(row['Service_Product_Name'], row['IAR_description'], ' - ')
                section_i.append(serv_desc)
            unique = []
            [unique.append(item) for item in section_i if item not in unique]
            service_desc_dict[i] = unique

        return service_desc_dict

    
    def systems_data(self):
        systems_dict = {}
        for i in self.ls:
            client_systems = self.prod_syst.loc[self.prod_syst['Parent_Product'] == i]['MappedSystem'].unique()
            syst_i = [item for item in client_systems]
            syst_desc_pairs = {}
            for j in syst_i:
                try:
                    syst_desc_pairs[j] = syst_desc.loc[j][0]

                except:
                    pass
            systems_dict[i] = syst_desc_pairs
        return systems_dict    

    
    def data_held(self):
        
        all_systems_used = {k: list(v.keys()) for k,v in self.systems_data().items()}
        data_per_section = {}

        for k,v in all_systems_used.items():
            items_per_section = []
            unique_items = []
            [items_per_section.extend(data_dict[item]) for item in v if item in data_dict]
            [unique_items.append(itm) for itm in items_per_section if itm not in unique_items]       
            data_per_section[k] = sorted(unique_items)
        return data_per_section
    
    def client_data(self):

        client_tables = {}
        
        client_tables['section_3_headings'] = self.section_3_headings()
        client_tables['section_1'] = self.gen_info
        client_tables['client_services'] = self.services_data()
        client_tables['client_systems'] = self.systems_data()
        client_tables['personal_data'] = self.data_held()
        
        return client_tables

# COMMAND ----------

ct = Client('Benenden Healthcare Pension Plan (Trustees of)')
ct.client_data()

# COMMAND ----------

ct1 = Client('A & H Group Pension Scheme (Trustees of)')
ct1.client_data()

# COMMAND ----------

# MAGIC %%time
# MAGIC test_data = {}
# MAGIC not_found = []
# MAGIC for client_name in accounts['Account_Name'][:]:
# MAGIC     
# MAGIC     try:
# MAGIC         ct = Client(client_name)
# MAGIC         test_data[client_name] = ct.client_data()
# MAGIC     except:
# MAGIC         not_found.append(client_name)

# COMMAND ----------

print(len(test_data), len(not_found))

# COMMAND ----------

not_found

# COMMAND ----------

ct2 = Client('Mackays Stores Limited')
ct2.client_data()

# COMMAND ----------

ct3 = Client('Northgate HR Pension Scheme (Trustees of)')
ct3.client_data()

# COMMAND ----------

from jinja2 import Template
from xhtml2pdf import pisa
import re
import html

pd.set_option('display.max_colwidth', -1)

# COMMAND ----------

# HTML Template example

html_string = ('<html>\
<head>\
<style> body {background-color: #f7f7f7} </style>\
<TITLE> {{ client }} </TITLE>\
</head>\
<body>\
<h1> {{ client }} </h1>\
<h1> Contents </h1>\
<h1> 1. Client Information </h1> {{section_1}} <p style="page-break-before: always">\
<p><h1> 2. Common System Information - Static Info </h1></p>\
<p style="page-break-before: always">\
<h1> 3. Service Information </h1> \
<p> The Products and Services set out below are for administrative purposes only and are not \
intended to be legally binding. Please consult your contract for detailed Product and \
Service descriptions </p>\
{{section_3}}\
<p><h1> 4. Organisational and Technical Security Measures - Static Info </h1></p>')


# COMMAND ----------

# Create a mount point for the pdf output files (must return true - run only once)

dbutils.fs.mount(
  source = "wasbs://<FOLDER_NAME>@<CONTAINER_NAME>.blob.core.windows.net/",
  mount_point = "/mnt/my_mount_directory_any_name",
  extra_configs = {"fs.azure.account.key.<ACCOUNT_NAME>.blob.core.windows.net" : "<ACCOUNT_KEY>"})

# COMMAND ----------

# Test the mount point is actually writeable (must return true)

# dbutils.fs.put("/mnt/my_mount_directory_any_name/1.txt", "Hello, World!", True)

dbutils.fs.ls('/mnt/my_mount_directory_any_name')

# COMMAND ----------

#old version 
'''
def sections(client_name):
    tables = test_data[client_name]
    section_1 = tables['section_1']
    section_3_headings = tables['section_3_headings']
    service_description = tables['client_services']
    it_systems = tables['client_systems']
    personal_data = tables['personal_data']
    return section_1, section_3_headings, service_description, it_systems, personal_data

def section_three(a,b,c,d):
    pd.set_option('display.max_columns', None)
    complete_string = ''
    for i in range(len(a)):
        str1 = '<p><h2> 3.' + str(i+1) + ' ' + str(list(a.items())[i][1]) + ' </h2></p>'
        str2 = '<p><b> Service Name: </b>' + str(list(a.items())[i][1]) + '</p>'
        str3 = '<p><b> Service Description (Products): </b>'  + str(pd.DataFrame.to_html(pd.DataFrame(list(b.items())[i][1], columns=["Service Description (Products)"]), index = False)) + '</p>'
        str4 = '<p> The following systems are used in the provision of this service </p> <p><b> IT Systems: </b>'  + str(pd.DataFrame.to_html(pd.DataFrame.from_dict((list(c.items())[i][1]), orient='index',columns=["Description"]))) + '</p>'
        str5 = '<p> Personal Data - Supplied by the Data Controller, the Data Subject and Authorised Third Parties. Actual Data held may not include al items below where individual contracts or data protection agreements exist. </p> <p><b> Personal Data Held: </b>'  + str(pd.DataFrame.to_html(pd.DataFrame(list(d.items())[i][1], columns=["Personal Data Held"]), index = False)) + '</p>'
        str6 = '<p style="page-break-before: always">'
        full_string = str1+str2+str3+str4+str5+str6
        complete_string = complete_string + full_string
    return complete_string

def convertHTMLtoPDF(sourceHTML, outputFilename):
    resultFile = open(outputFilename, "w+b")
    pisaStatus = pisa.CreatePDF(sourceHTML, dest=resultFile)
    resultFile.close()
    return pisaStatus.err
'''

# COMMAND ----------

# NEW VERSION

def sections(client_name):
    tables = test_data[client_name]
    section_1 = tables['section_1']
    section_3_headings = tables['section_3_headings']
    service_description = tables['client_services']
    it_systems = tables['client_systems']
    personal_data = tables['personal_data']
    return section_1, section_3_headings, service_description, it_systems, personal_data

def contents(client_name):
    contents_string=''
    for heading in range(len(sections(client_name)[1])):
        str1='<li> ' + str(sections(client_name)[1][heading+1]) + '</li>'
        contents_string = contents_string + str1
    return contents_string

def section_1(client_name):
    client_info_column1 = list(sections(client_name)[0].keys())
    client_info_column2 = list(sections(client_name)[0].values())
    client_info_dataframe1 = pd.DataFrame({'Details held by CES': client_info_column1[0:2], '': client_info_column2[0:2]})
    client_info_dataframe2 = pd.DataFrame({'CES Details': client_info_column1[2:6], '': client_info_column2[2:6]})
    client_info_dataframe3 = pd.DataFrame({'Client Details': client_info_column1[6:end], '': client_info_column2[6:end]})
    client_info_html = pd.DataFrame.to_html(client_info_dataframe1, index = False).replace('<thead>','<thead style = "text-align: right">') + pd.DataFrame.to_html(client_info_dataframe2, index = False).replace('<thead>','<thead style = "text-align: right">') + pd.DataFrame.to_html(client_info_dataframe3, index = False).replace('<thead>','<thead style = "text-align: right">')
    return client_info_html

def section_three(headings,products,itsystems,personaldata):
    complete_string = ''
    for i in range(len(headings)):
        str1 = '<p><h2> 3.' + str(i+1) + ' ' + str(list(headings.items())[i][1]) + ' </h2></p>'
        str2 = '<p><b> Service Name: </b>' + str(list(headings.items())[i][1]) + '</p>'
        str3 = '<p>'  + str(pd.DataFrame.to_html(pd.DataFrame(list(products.items())[i][1], columns=["Service Description (Products)"]), index = False)) + '</p>'
        str4 = '<p> <b> IT Systems: </b> The following systems are used in the provision of this service. '  + str(pd.DataFrame.to_html(pd.DataFrame(list(list(itsystems.items())[i][1].items()), columns=['IT System','Description']).sort_values('IT System'), index=False).replace('<th>IT System','<th style = "width:30%"> IT System').replace('<th>Description','<th style = "width:70%"> Description')) + '</p>'
        str5 = '<p> <b> Personal Data: </b> Supplied by the Data Controller, the Data Subject and Authorised Third Parties. Actual Data held may not include all items below where individual contracts or data protection agreements exist. </p>'  + str(pd.DataFrame.to_html(pd.DataFrame(list(personaldata.items())[i][1], columns=["Personal Data Held"]), index = False, justify="left").replace('<tbody>','<tbody style = "text-align: left">').replace('<table', '<table width = "50%"')) + '</p>'
        str6 = '<p style="page-break-before: always">'
        complete_string = complete_string + str1+str2+str3+str4+str5+str6
    return complete_string

def convertHTMLtoPDF(sourceHTML, outputFilename):
    resultFile = open(outputFilename, "w+b")
    pisaStatus = pisa.CreatePDF(sourceHTML, dest=resultFile)
    resultFile.close()
    return pisaStatus.err

# COMMAND ----------

html_string = ('''<html>
<head>
    <style>

    h1 {font-weight:normal; 
        color: rgb(0,161,192); 
        font-family:arial; 
        font-size:270%; 
        margin-bottom: 0px;
        }
    h2 {font-weight:normal; 
        color: rgb(0,161,192); 
        font-family:arial; 
        font-size:200%; 
        margin-bottom: 0px;
        }
    p {font-size:120%; 
        line-height: 1.2
        }

    table, th, td {-pdf-keep-with-next: false; 
        border: 0.5px solid rgb(0,91,130); 
        vertical-align: top; 
        font-size: 105%; 
        line-height: 1.2
        }
    th {background-color: rgb(0,91,130); 
        color: white; 
        padding-top: 3px; 
        padding-bottom: 1.3px
        }
    td {padding-top: 3px; 
        padding-left: 2px; 
        padding-bottom:1.2px
        }

    .left {float:left;}
    .right {float:right;}
    .center {float:center;}
    
    .description{
        color: black;
    }
    .description em {
        color: blue;
    }

    ol.main {list-style-type:decimal; 
        font-size: "130%"; 
        line-height: 1.2;}
    OL {counter-reset: item}
      LI {display: block}
      LI:before {content: counters(item, ".") " "; counter-increment: item}

    ul.main {font-size: "120%";
        line-height: 1.2}
    ul.second {font-size: "110%";
        line-height: 1.2}

    hr {background-color: rgb(0,91,130); 
        border-color: rgb(0,91,130); 
        height: 1.5px; 
        border: 0; 
        }

    @page {
        size: a4 portrait;
        @frame header_frame {
            /* Content Frame */ 
            left: 50pt; 
            width: 420pt; 
            top: 50pt; 
            height: 672pt; 
            margin:0
            }
        @frame footer_frame {
            /* Static frame */ 
            -pdf-frame-content: footerFirst; 
            left: 45pt; 
            width: 490pt; 
            top: 807pt; 
            height: 20pt;
            }
        @frame page_number_frame {
            /* Static frame */ 
            -pdf-frame-content: pageNumber; 
            left:530pt; 
            width: 530pt; 
            top: 807pt; 
            height: 20pt
            }
        }

    @page secondpage {
        size: a4 portrait;
        @frame header_frame {
            /* Content Frame */ 
            left: 50pt; 
            width: 490pt; 
            top: 50pt; 
            height: 672pt; 
            margin:0
            }
        @frame bottom_frame {
            /* Bottom Frame */ 
            -pdf-frame-content: bottom_content; 
            left: 50pt; 
            width: 490pt; 
            top: 720pt; 
            height: 672pt; 
            margin:0
            }
        @frame footer_frame {
            /* Static frame */ 
            -pdf-frame-content: footer_content; 
            left: 45pt; 
            width: 490pt; 
            top: 807pt; 
            height: 20pt;
            }
        @frame page_number_frame {
            /* Static frame */ 
            -pdf-frame-content: pageNumber; 
            left:530pt; 
            width: 530pt; 
            top: 807pt; 
            height: 20pt
            }
        }

    @page innerpages {
        size: a4 portrait;
        @frame header_frame {
            /* Content Frame */ 
            left: 50pt; 
            width: 490pt; 
            top: 50pt; 
            height: 700pt; 
            margin:0
            }
        @frame footer_frame {
            /* Static frame */ 
            -pdf-frame-content: footer_content; 
            left: 45pt; 
            width: 490pt; 
            top: 807pt; 
            height: 20pt;
            }
        @frame page_number_frame {
            /* Static frame */ 
            -pdf-frame-content: pageNumber; 
            left:530pt; 
            width: 530pt; 
            top: 807pt; 
            height: 20pt
            }
        }

    </style>
</head>



<body>

    <div id="footerFirst">
        COMMERCIAL IN CONFIDENCE
    </div>
    <div id="pageNumber">
        <pdf:pagenumber>
    </div>

    <!--img src = "dbfs:/mnt/my_mount_directory_any_name/cpt_name.png"-->
    <p></p>
    <!--img src = "dbfs:/mnt/my_mount_directory_any_name/cpt_report_logo.png"-->
    <h1 style = "color: rgb(0,91,130); font-family:Helvetica; font-size:350%;"> cpt Employee Solutions (CES) Information Asset Register - Data Processing Records </h1>
    <h1 style = "color: rgb(0,91,130); font-family:Helvetica; font-size:300%;"> {{ client }} </h1>

    <pdf:nexttemplate name="secondpage">
        <div id="footer_content">
            Information Asset Register prepared by cpt
        </div>
        <div id="pageNumber">
            <pdf:pagenumber>
        </div>
        <div id="bottom_content">
            <!--img src = "dbfs:/mnt/my_mount_directory_any_name/cpt_investor_in_customers.png"-->
        </div>

    <p style="page-break-before: always"> Prepared by: </p>
    <p>### User details ###</p>
    <p style="color:blue;"> <u> www.cptemployeesolutions.co.uk </u></p>
    <hr>
    <!--img src = "dbfs:/mnt/my_mount_directory_any_name/cpt_awards.png"-->
    <hr>

    <pdf:nexttemplate name="innerpages">
        <div id="footer_content">
            Information Asset Register prepared by cpt
        </div>
        <div id="pageNumber">
            <pdf:pagenumber>
        </div>

    <h1 style=\"page-break-before: always\"> Contents </h1>

    <ol class="main">
        <li> Client Information </li>
        <li> Common System Information </li>
        <li> Service Information 
            <ol>
                {{dynamic_contents}}
            </ol>
        </li>
        <li> Organisational and Technical Security Measures </li>
        <li> Transfers outside the EEA </li>
        <li> Data Retention and Deletion Policy </li>
        <li> Suppliers, Subprocessors </li>
        <li> Legal Basis for Processing </li>
    </ol>
    <p class="description">cpt Employee Benefits has recently merged with cpt HRS solutions to form cpt Employee Solutions (CES). cpt Employee Solutions is part of cpt plc. More information can be found on: <em><u> https://www.cptemployeesolutions.co.uk/who-we-are/ </u></em></p>
    <p>cpt Employee Solutions (CES) is the trading name for:</p>
    <ul class="main">
        <li>cpt Employee Benefits Limited (CEBL)</li>
        <li>cpt Employee Benefits (Consulting) Limited CEB(C)L</li>
        <li>cpt Business Services Limited (CBSL)</li>
    </ul>

    <p><h1 style="page-break-before: always"> 1. Client Information </h1> {{section_1}} </p>


    <h1 style="page-break-before: always"> 2. Common System Information </h1>
    <p>The following common systems are used in the provision of Services to all Clients.</p>
    <p><b> IT Systems </b></p>

    <table>
        <thead>
            <tr>
                <th style = \"width:30%\"> IT System </th>
                <th style = \"width:70%\"> Description </th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td> cpt Data Backup System </td>
                <td> Processes and stores backups of systems used. </td>
            </tr>
            <tr>
                <td> cpt Internal Emails </td>
                <td> MS Outlook – Information manager mainly used for email and calendars. </td>
            </tr>
            <tr>
                <td> cpt Serve File Data </td>
                <td> Shared drives to save documents on. </td>
            </tr>
            <tr>
                <td> CIBLink </td>
                <td> System used for handling and tracking any complaints. </td>
            </tr>
            <tr>
                <td> Microsoft Office </td>
                <td> Microsoft Office suite including Word, Excel, Access, PowerPoint, One Note. </td>
            </tr>
            <tr>
                <td> Pardot </td>
                <td> Marketing mailing system used for tracking prospect actions. Used by Marketing and Sales. </td>
            </tr>
            <tr>
                <td> Salesforce </td>
                <td> CRM (Client Relationship Management) System used for managing client relationships and the Lead to Cash cycle. Used for recording and tracking client interactions. MI Reporting for Sales, Marketing. Manages business critical processes like compliance and retention. </td>
            </tr>
            <tr>
                <td> SAP </td>
                <td> Used by the cpt Finance Team to report and manage corporate accounts and finance. </td>
            </tr>
            <tr>
                <td> SharePoint </td>
                <td> Document management and storage system. </td>
            </tr>
            <tr>
                <td> SQL Server </td>
                <td> Database management system. </td>
            </tr>
            <tr>
                <td> Voice Recordings </td>
                <td> Voice recordings from phonecalls. </td>
            </tr>
        </tbody>
    </table>

    <p> Personal Data - Supplied by the Data Controller, the Data Subject and Authorised Third Parties. Actual data held may not include all items below where individual contracts or data protection agreements exist. </p>

    <table width = "50%";>
        <thead>
            <tr>
                <th> Personal Data Held </th>
            </tr>
        </thead>
        <tbody style = "text-align: left";>
            <tr>
                <td> Address </td>
            </tr>
            <tr>
                <td> Bank Details </td>
            </tr>
            <tr>
                <td> DOB </td>
            </tr>
            <tr>
                <td> Email Address </td>
            </tr>
            <tr>
                <td> Gender </td>
            </tr>
            <tr>
                <td> Location Data e.g. GPS Data </td>
            </tr>
            <tr>
                <td> Maiden Name </td>
            </tr>
            <tr>
                <td> Name </td>
            </tr>
            <tr>
                <td> NI Number </td>
            </tr>
            <tr>
                <td> Online Identifier e.g. IP Address </td>
            </tr>
            <tr>
                <td> Phone Number </td>
            </tr>
            <tr>
                <td> Postcode </td>
            </tr>
            <tr>
                <td> Salary </td>
            </tr>
            <tr>
                <td> Scanned Documents </td>
            </tr>
        </tbody>
    </table>


    <p><h1 style="page-break-before: always"> 3. Service Information </h1></p>
    <p> The Products and Services set out below are for administrative purposes only and are not intended to be legally binding. Please consult your contract for detailed Product and Service descriptions. </p>
    {{section_3}}


    <p><h1 style="page-break-before: auto"> 4. Organisational and Technical Security Measures </h1></p>
    <p><i>Data is held electronically in cpt secure data centres and manual records are kept in secure cpt locations.</i></p>
    <p><b>Overview of Security Measures</b></p>
    <p>Most CES sites and all cpt Data Centres are certified to the ISO 27001 standard due to their professionally managed and highly proactive Information Security Management Systems (ISMS). Any sites which are not certified, are still required to operate to ISO standards. The scope of our accreditation includes: pension schemes administration, pensions payroll, fund accounting, tracing solutions services and the design, development, provision and support of pension administration systems undertaken by cpt. It includes all activities that hold, obtain, record, use, share or manage client, employee and corporate, information or data. Security controls at data centres and administration delivery locations are assessed against the ISO 27001 standard on an annual basis. All servers have vulnerability assessments using auto-assessment tools. Ad hoc vulnerability assessments are carried out throughout the year. Annual third party CESG CHECK accredited penetration tests are performed against external facing systems. Frequent client penetration testing is conducted which covers all aspects of penetration e.g. networks, scanning etc. All ingress points are protected by multi-vendor, multi-tiered firewalls as mandated by cpt’s Security Policy.</p>
    <p>All machines are covered by cpt’s corporate antivirus solution. McAfee software is installed on all machines and managed centrally by cpt IT Services (ITS) with the latest patches applied daily. Reports are also produced daily to ensure that all machines are running the latest version of the software. The local IT Support Team are notified of any virus alerts, which results in thorough investigation, in conjunction with our Security Manager. All cpt laptops are subject to mandatory full-disk encryption using McAfee Drive Encryption which is certified for FIPS 140-2, Common Criteria EAL2+, and Intel Advanced Encryption Standard–New Instructions (AES–NI).Removable devices (such as data sticks, CDs, portable storage devices) are not used for storing operational customer data i.e. database files, records of client information, log-in details for systems etc. locally. We use cpt approved removable media devices to transfer data internally and externally, client and group approvals must be needed to transfer data. Only authorised users can write to removable media.</p>
    <p>All users have to recertify that their access is still required annually. To limit CES exposure to data leakage, cpt utilise Sanctuary device control for managing access to removable devices such as DVDs, CDs, USB flash drives and external hard drives. The software suite uses AES 256 bit encryption that is FIPS 140-2 Level 2 compliant (includes physical tamper-evidence and role-based authentication requirements) that allows CES to enforce the use of encryption on all removable media devices (DVDs, CDs and USB devices). Access is controlled using AD group membership and follows a defined process which requires approval from cpt Insurance and Solutions Divisional Security and Group Security (if requests involve IL3 data). By default, this is configured to Read-Only mode, which allows users to view the contents of the media, but prohibits them from copying data to the device. Device control has been configured such that access to Wi-Fi cards is denied when laptops are connected to the corporate network. The cpt Information Security Team subscribes to third party monitoring and alerting of vulnerabilities and new threats. cpt is a member of the Government Service Providers Forum which also issues alerts. We also subscribe to manufacturers’ alerting systems. Our Information Security Team frequently communicates with peers across cpt to discuss and assess threats and vulnerabilities.</p>
    <p>Data back-up and recovery processing is also in place for this data.</p>


    <p><h1 style="page-break-before: always"> 5. Transfers outside the EEA </h1></p>
    <p>All CES Personal Data Processing, including that undertaken by our sub-processors is executed within the EEA unless agreed otherwise by the Data Controller.</p>


    <p><h1> 6. Data Retention and Deletion Policy </h1></p>
    <p class="description">A copy of our Data Retention Statement is being issued with our Letters of Variation. Outside of our standard approach, as detailed in our Data Retention Statement. The data retention statement can also be viewed via the following link: <em><u> https://www.cptemployeesolutions.co.uk/cookies-privacy </em></u></p>


    <p><h1> 7. Suppliers, Subprocessors </h1></p>
    <p>CES has a number of suppliers/sub-processors.</p>
    <p><b>Internal Suppliers</b></p>
    <p>Internal suppliers are suppliers who are part of cpt Plc. The following internal suppliers are used in relation to the services provided by CES:</p>
    <ul class="second">
        <li> cpt Document and Information Services (CDIS/CIC) – CDIS/CIC provide Print Services, Ingoing and Outgoing Communication Services and Document Storage Services (including document retrieval, archiving and destruction) </li>
        <li> cpt IT Enterprise Services (cpt ITES) – cpt ITES provide Data Centres, Security Services, IT Infrastructure and IT Hardware. </li>
    </ul>
    <p><b>External Suppliers</b></p>
    <p>We are engaging with external suppliers to ensure updated contract clauses are implemented and that they have taken appropriate measures to ensure they are GDPR compliant. Additional information regarding suppliers will be incorporated into future versions of the IAR.</p>


    <h1> 8. Legal Basis for Processing </h1></p>
    <p>As confirmed in the cpt Services Agreement, CES is a Data Processor as defined by the Data Protection Act and GDPR and will carry out the processing defined in the agreement with the Client. Any Rights of the Data Subject requests will be actioned in consultation with Data Controllers. The Legal Basis for Processing refers to the Data Controllers underlying justification for processing the data subject’s personal data. This is to be defined by the Client, in line with the valid legal bases defined by GDPR and is to be confirmed by the Client who is acting as the Data Controller, to the Data Processor (CES).</p>
    <p><b>LEGAL DISCLAIMER:</b> This information is provided as part of cpt’s compliance with the General Data Protection Regulation (“GDPR”) and is based on the personal data that cpt are processing on your behalf. Therefore, the information is not intended to be full or complete and should not be relied upon by you. As a Data Controller it is your duty to document your own record of processing activities as required by the GDPR: cpt shall have no liability to you in your use of this document.</p>


</body>
''')

# COMMAND ----------

''' OLD VERSION



for client_name in ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee']:
    client_text = re.sub(r"\W","",client_name,flags=re.I)
    outputFilename = (r"/dbfs/mnt/my_mount_directory_any_name/" + str(client_text) + '.pdf')
    section_1_html = pd.DataFrame.to_html(pd.DataFrame.from_dict(sections(client_name)[0], orient='index',columns=["Description"]))
    section_3_html = section_three(sections(client_name)[1], sections(client_name)[2], sections(client_name)[3], sections(client_name)[4])
    template = Template(html_string)
    html_string_rendered = template.render(client = client_name, section_1 = section_1_html, section_3 = section_3_html)
    convertHTMLtoPDF(html_string_rendered, outputFilename)
    
    '''

# COMMAND ----------

list(accounts.columns.values)

# COMMAND ----------

for client_name in accounts["Account_Name"][:]:
    client_text = re.sub(r"\W","",client_name,flags=re.I)
    outputFilename = (r"/dbfs/mnt/my_mount_directory_any_name/" + str(client_text) + '.pdf')
    print("saving ..."+outputFilename)
    contents_html = contents(client_name)
    client_info_column1 = list(sections(client_name)[0].keys())
    client_info_column2 = list(sections(client_name)[0].values())
    client_info_dataframe1 = pd.DataFrame({'Details held by CES': client_info_column1[0:2], '': client_info_column2[0:2]})
    client_info_dataframe2 = pd.DataFrame({'CES Details': client_info_column1[2:6], '': client_info_column2[2:6]})
    client_info_dataframe3 = pd.DataFrame({'Client Details': client_info_column1[6:end], '': client_info_column2[6:end]})
    section_1_html = pd.DataFrame.to_html(client_info_dataframe1, index = False) + pd.DataFrame.to_html(client_info_dataframe2, index = False) + pd.DataFrame.to_html(client_info_dataframe3, index = False)
    section_3_html = section_three(sections(client_name)[1], sections(client_name)[2], sections(client_name)[3], sections(client_name)[4])
    template = Template(html_string)
    html_string_rendered = template.render(client = client_name, dynamic_contents = contents_html, section_1 = section_1_html, section_3 = section_3_html)
    convertHTMLtoPDF(html_string_rendered, outputFilename)
