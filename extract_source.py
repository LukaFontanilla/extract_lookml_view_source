import requests
import configparser
import base64
import lkml
import json

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# read credentials
config2 = configparser.ConfigParser()
config2.read(r'path to ini file')
GITHUB = str(config2['ini file profile to initialize']['access token'])

headers2 = {'Authorization': "token " + GITHUB}


# get file from repo
def getDTS(dir_url):
  try:
    repo_url = dir_url
    file = requests.get(repo_url, headers=headers2, verify=False)
    json2 = file.json()
  except:
    print("Failed to fetch url")

  # decode file to dict string
  content = base64.b64decode(json2['content'])
  string = content.decode('utf-8')

  # parse lookml with parser
  parsed = lkml.load(string)
  
  try:
    if 'derived_table' in parsed['views'][0] and ('explore_source' in parsed['views'][0]['derived_table'] or 'sql_create' in parsed['views'][0]['derived_table']):
      return 'Explore Source: ' + parsed['views'][0]['derived_table']['explore_source']['name']
    elif 'sql_table_name' in parsed['views'][0]:
      return 'SQL_TABLE_NAME: ' + parsed['views'][0]['sql_table_name']
    else:
      return 'SQL Derived Table: ' + parsed['views'][0]['derived_table']['sql']
  except Exception as e:
    return e

### Use base contents url to extract all paths in repo

repo_url2 = "base repo url ending in /contents/"
file = requests.get(repo_url2, headers=headers2, verify=False)
json3 = file.json()

### send all the possible paths to the array to loop through

paths = []

for directory in json3:
  if '.' not in directory['path']:
    paths.append(directory['path'])
  else:
    pass

print(paths)

### loop through and call getDTS function on each view file in the directory
view=[]
dt_sql=[]

for path in paths:
  dir_url = "base url from earlier" + path
  file = requests.get(dir_url, headers=headers2, verify=False)
  file_json = file.json()
  
  for files in file_json:
    if '.' not in files['path']:
      sub_dir_url = files['url']
      sub_dir_file = requests.get(sub_dir_url, headers=headers2, verify=False)
      sub_dir_files_json = sub_dir_file.json()
      
      for sub_file in sub_dir_files_json:
        if 'view.lkml' in sub_file['name']:
          sub_file_name = sub_file['name'].split('.')
          view.append(sub_file_name[0])
          dt_sql.append(getDTS(dir_url=sub_file['url']))
        elif '.gitkeep' in sub_file['path'] or 'model' in sub_file['path'] or 'dashboard' in sub_file['path']:
          pass
        else:
          sub_file_name = sub_file['name'].split('.')
          view.append(sub_file_name[0])
          dt_sql.append('Not a View')
    else:
      if 'view.lkml' in files['path']:
        view_name = files['name'].split('.')
        view.append(view_name[0])
        dt_sql.append(getDTS(dir_url=files['url']))
      elif '.gitkeep' in files['path'] or 'model' in files['path'] or 'dashboard' in files['path']:
        pass
      else:
        view_name = files['name'].split('.')
        view.append(view_name[0])
        dt_sql.append('Not a View')


# create dataframe
view_df = pd.DataFrame({"File Name":view,
                        "Source":dt_sql
                   })

print(view_df)
