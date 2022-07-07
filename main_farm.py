import requests
import json
import pandas as pd
import psycopg2 as p
import pandas.io.sql as psql
from pathlib import Path

#Token is at the settings pane of the Kobo toolbox; near the logout menu
TOKEN = '072d1667b69adf1d7fd85de61f2072d40715f186'

#We need the headers as a means to login to Kobo toolbox server from third party tools
headers = {"Authorization": f'Token {TOKEN}'}

KF_URL = 'kobo.humanitarianresponse.info'
#Those using non-humanitaria Kobo toolbox server should use the end point https://kc.kobotoolbox.org/api/v1/

#Asset Id retrived from kobo toolbox; click on a form then retrieve the value between forms and summary
FARMER_ASSET_UID = 'aZZY7pySEe4AuSy4SefDWR'
FARM_ASSET_UID = 'awiMWPiKDcmRfgT9BVPx8M'

#JSON based URLs for the two forms
FARMER_URL = f'https://{KF_URL}/api/v2/assets/{FARMER_ASSET_UID}/data/?format=json'
FARM_URL = f'https://{KF_URL}/api/v2/assets/{FARM_ASSET_UID}/data/?format=json'

#postgres database connection
connection = p.connect(user="postgres",
                               password="Maun2806;",
                               host="127.0.0.1",
                               port="5432",
                               database="kadgi")

cursor = connection.cursor()

#Fetch farmers from kobo toolbox and parse the JSON file saving to posgres database
farmer_response = requests.get(FARMER_URL, headers=headers)

farmer_object=farmer_response.text
parsed_farmer=json.loads(farmer_object)
for farmer in parsed_farmer['results']:
    combined_name=farmer['farm_registration/first_name']+" "+farmer['farm_registration/middle_name']
    save_farmer="insert into public.farmers(farmer_name,father_husband_name,combine_name,village,gram_panchayat,district)SELECT \
                 '"+farmer['farm_registration/first_name']+"', \
                 '"+farmer['farm_registration/middle_name']+"', \
                 '"+combined_name+"', \
                 '"+farmer['farm_registration/village']+"', \
                 '"+farmer['farm_registration/gram_panchayat']+"', \
                 '"+farmer['farm_registration/district']+"' \
                 where not exists (select * from public.farmers WHERE combine_name='"+combined_name+"' and district='"+farmer['farm_registration/district']+"')"
    cursor.execute(save_farmer)
    connection.commit() 

#Fetch details of farms that already have interviews done for them
farm_response = requests.get(FARM_URL, headers=headers) 
#process the farms interviews have already been done for and discount in the CSV produced for the pull data functionality
farm_object=farm_response.text
parsed_farm=json.loads(farm_object)
for farm in parsed_farm['results']:
    update_completed= "update public.farmers set completed ='YES' where combine_name ='"+farm['farm_registration/farmer_name']+"' "
    cursor.execute(update_completed)
    connection.commit()

farmers_list = psql.read_sql("SELECT * from public.farmers where completed IS NULL",connection)
farmers_list.to_csv("farmers_db.csv",index=False) 

#now change the new CSV for the farm survey

XFORM = 1126741
#xform is theunique id kobo assigned the form we wish to update the CSV media file for
KC_URL = 'https://kc.humanitarianresponse.info/api/v1/'
#We need the KC URL to update media files

#While in an active login session for Kobo toolbox
#use the URLs below to get the XForm value that corresponds with a form of interest
#https://kc.kobotoolbox.org/api/v1/metadata.json
#https://kc.humanitarianresponse.info/api/v1/forms.json


#Details specific to CSV media file
FILE_FOLDER = '.'  #.means we will be processing the media file from the current directory
FILENAME = 'farmers_db.csv'
MIME = 'text/csv'

#The portion below handles the gist of the CSV media replacement
headers = {'Authorization': f'Token {TOKEN}'}
files = {'data_file': (FILENAME, open(fr'{FILE_FOLDER}\{FILENAME}', 'rb').read(), MIME)}
data = {
        'data_value': FILENAME,
        'xform': XFORM,
        'data_type': 'media',
        'data_file_type': MIME,
    }

    # Download metadata.json
response = requests.get(fr"{KC_URL}/metadata.json", headers=headers)
dict_response = json.loads(response.text)

    # Delete appropriate entry in the metadata.json (delete old file)
for each in dict_response:
        if each['xform'] == XFORM and each['data_value'] == FILENAME:
            del_id = each['id']
            response = requests.delete(fr"{KC_URL}/metadata/{del_id}", headers=headers)
            break

# Upload the changed file
response = requests.post(fr"{KC_URL}/metadata.json", data=data, files=files, headers=headers)

print(response)
