import requests
import re 
import json
import pandas as pd
import os 

webpages = []

params = {
    "engine": "google_jobs",
    "q": "data analyst",
    "location": "United States",
    "api_key": os.getenv("SERPAPI_API_KEY")
}

for num in range(0, 100, 10):

    response = requests.get("https://serpapi.com/search", params=params)
    webpage = response.json()

    try:
        params['next_page_token'] = webpage['serpapi_pagination']['next_page_token']
        webpages.append(webpage)
    except:
        pass
    
try:
    with open('webpages.json', 'r') as f:
        existing_data = json.load(f)
    combined_data = existing_data + webpages
except FileNotFoundError:
    combined_data = webpages  

with open('webpages.json', 'w') as job_export:
    json.dump(combined_data, job_export)

with open('webpages.json', 'r') as job_export:
    loaded_webpages = json.load(job_export)

keywords = [
    r"\bSQL\b",
    r"\bC#\b",
    r"\bC\+\+\b",
    r"\bETL\b",
    r"\bData Mining\b",
    r"\bMachine Learning\b",
    r"\bAI\b",
    r"\bChatGPT\b",
    r"\bPowerBI\b",
    r"\bPower BI\b",
    r"\bExcel\b",
    r"\bPython\b",
    r"\bR\b",
    r"\bSAS\b",
    r"\bGIT\b",
    r"\bTableau\b",
    r"\bAgile\b",
    r"\bPowerPoint\b",
    r"\bMicrosoft Office\b",
    r"\bSAP\b",
    r"\bOracle\b",
    r"\bNLP\b",
    r"\bNeural Networks\b",
    r"\bSPSS\b",
    r"\bMATLAB\b",
    r"\bC\b",
    r"\bAWS\b",
    r"\bJava\b",
    r"\bJavaScript\b",
    r"\bSpark\b",
    r"\bHadoop\b",
    r"\bTensorFlow\b",
    r"\bScala\b",
    r"\bJulia\b",
    r"\bSuper AI\b",
    r"\bMy SQL\b",
    r"\bPostgreSQL\b",
    r"\bSQL Server\b",
    r"\bSQLite\b",
    r"\bAzure\b",
    r"\bLooker\b",
    r"\bQlik\b",
    r"\bPandas\b",
    r"\bNumpy\b",
    r"\bKeras\b",
    r"\bPytorch\b",
    r"\bSnowflake\b",
    r"\bDatabricks\b",
    r"\bVBA\b",
    r"\bVLOOKUP\b",
    r"\bXLOOKUP\b",
    r"\bSalesForce\b",
    r"\bBigQuery\b",
    r"\bFlask\b",
    r"\bDjango\b",
    r"\bScikit\b",
    r"\bLLM\b",
    r"\bGit\b",
    r"\bJira\b",
    r"\bConfluence\b"
]

job_ids = []
result_dict = {}

highlight_len = 0

for entry in loaded_webpages:
        for key in entry['jobs_results']:
                if key['job_id'] not in job_ids:
                        job_ids.append(key['job_id'])
                        for keyword in keywords:
                                try:
                                        highlight = ', '.join(f"{keys}: {value}" for d in key['job_highlights'] for keys, value in d.items()).lower().replace('powerbi', 'power bi')
                                        highlight_len = len(re.findall(keyword.lower(), highlight))

                                except:
                                        pass
                
                                desc_len = len(re.findall(keyword.lower(), key['description'].lower().replace('powerbi', 'power bi')))

                                if desc_len > 0 or highlight_len > 0:
                                        if keyword.replace(r'\b', '') not in result_dict:
                                                result_dict[keyword.replace(r'\b', '')] = 1

                                        else:
                                                result_dict[keyword.replace(r'\b', '')] += 1

job_id_df = {}

for entry in loaded_webpages:
        for key in entry['jobs_results']:
                if key['job_id'] not in job_id_df:
                        try:
                                dental_insurance = key['detected_extensions']['dental_coverage']
                        except: 
                                dental_insurance = False
                        try:
                                health_insurance = key['detected_extensions']['health_insurance']
                        except:
                                health_insurance = False
                        try:
                                schedule_type = key['detected_extensions']['schedule_type']
                        except:
                                schedule_type = pd.NA
                        try:
                                salary = key['detected_extensions']['salary']
                        except:
                                salary = pd.NA 
                        try:
                                wfh = key['detected_extensions']['work_from_home']
                        except:
                                wfh = pd.NA 
                        try:
                                pto = key['detected_extensions']['paid_time_off']
                        except:
                                pto = pd.NA
                        try:
                                quals = key['detected_extensions']['qualifications']
                        except:
                                quals = pd.NA

                        job_id_df[key['job_id']] = {'Location': key['location'],
                                                      'Company Name': key['company_name'],
                                                      'Schedule': schedule_type,
                                                      'Medical Insurance': health_insurance,
                                                      'Dental Insurance': dental_insurance,
                                                      'Salary': salary,
                                                      'Work from Home': wfh,
                                                      'Paid Time Off': pto,
                                                      'Qualifications': quals}

result_dict = pd.DataFrame.from_dict(dict(sorted(result_dict.items(), key=lambda x: x[1], reverse=True)), orient='index')
result_dict = result_dict.rename(columns={0:'Count'})
result_dict['Count'] = result_dict['Count']/(len(loaded_webpages)*10)

jobs_df = pd.DataFrame.from_dict(job_id_df, orient='index')
jobs_df = jobs_df.reset_index(drop=True)

result_dict.to_csv("skill_file.csv")
jobs_df.to_csv("job_file.csv")
