import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import tqdm
from airflow.models import Variable

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']
links = [[] for i in range(len(sources))]
links_dataframe = None

# this function extracts all of the links from the sources
def extract():

    links_dataframe = None    
    
    for i, source in enumerate(sources):
        reqs = requests.get(source)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        links[i] = [a['href'] for a in soup.find_all('a', href=True)]

    links_dataframe = pd.DataFrame(columns=['Source', 'Links'])

    for i, source in enumerate(sources):
        links_dataframe = links_dataframe._append({'Source': source, 'Links': links[i]}, ignore_index=True)    
    
    # only keep top 20 links for speed
    links_dataframe['Links'] = links_dataframe['Links'].apply(lambda x: x[:20])

    # clean the links dataframe to remove any empty links
    links_dataframe['links'] = links_dataframe['Links'].apply(lambda x: len(x) > 0)

    # remove any duplicate links
    links_dataframe['Links'] = links_dataframe['Links'].apply(lambda x: list(set(x)))

    # remove incomplete links
    links_dataframe['Links'] = links_dataframe['Links'].apply(lambda x: [link for link in x if link.startswith('http')])


    # extract titles and descriptions of the links and preprocess them
    data = pd.DataFrame(columns=['Link', 'Title', 'Description'])
    for i, source in enumerate(sources):
        for link in tqdm.tqdm(links_dataframe.loc[i, 'Links']):
            reqs = requests.get(link)
            if reqs.status_code != 200:
                continue
            soup = BeautifulSoup(reqs.text, 'html.parser')
            title = soup.find('title')
            title = title.get_text() if title else ''
            
            if title == None or title == '' or title == ' ':
                continue

            description = " ".join([p.get_text() for p in soup.find_all('p')])
            if description == None or description == '' or description == ' ':
                continue

            data = data._append({'Link': link, 'Title': title, 'Description': description}, ignore_index=True)

    links_dataframe = data

    print(links_dataframe.head())

    Variable.set("links_dataframe", links_dataframe.to_json(), serialize_json=True)

    

# this function extracts the titles and decriptions then preprocesses them
def transform(**kwargs):

    links_dataframe = Variable.get("links_dataframe", deserialize_json=True) # data from step 1

    if type(links_dataframe) != pd.DataFrame:
        links_dataframe = pd.read_json(links_dataframe)

    print("Transformation")

    # process the titles
    links_dataframe['Title'] = links_dataframe['Title'].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    links_dataframe['Title'] = links_dataframe['Title'].apply(lambda x: x.replace('\n', ' '))
    links_dataframe['Title'] = links_dataframe['Title'].apply(lambda x: x.replace('\r', ' '))
    links_dataframe['Title'] = links_dataframe['Title'].apply(lambda x: x.replace('\t', ' '))
    
    # preprocess description to remove symbols
    links_dataframe['Description'] = links_dataframe['Description'].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    links_dataframe['Description'] = links_dataframe['Description'].apply(lambda x: x.replace('\n', ' '))
    links_dataframe['Description'] = links_dataframe['Description'].apply(lambda x: x.replace('\r', ' '))
    links_dataframe['Description'] = links_dataframe['Description'].apply(lambda x: x.replace('\t', ' '))


    # remove any rows with empty titles or descriptions
    links_dataframe = links_dataframe[links_dataframe['Title'] != '']
    links_dataframe = links_dataframe[links_dataframe['Description'] != '']

    # make titles lowercase
    links_dataframe['Title'] = links_dataframe['Title'].apply(lambda x: x.lower())
    
    # make descriptions lowercase
    links_dataframe['Description'] = links_dataframe['Description'].apply(lambda x: x.lower())

    print(links_dataframe.head())
    
    Variable.set("links_dataframe", links_dataframe.to_json(), serialize_json=True) # update data for step 3

def load(**kwargs):

    links_dataframe = Variable.get("links_dataframe", deserialize_json=True) # get data from step 2

    if type(links_dataframe) != pd.DataFrame:
        links_dataframe = pd.read_json(links_dataframe)

    print("Loading")

    # create csv dataset
    links_dataframe.to_csv('/home/fasih/i200432_MLOps_A2/data/links.csv', index=False)
    print("Data saved to links.csv")

    # version the dataset and push to dvc
    commands = ['cd /home/fasih/i200432_MLOps_A2/', 'dvc add ./data/links.csv', 'dvc push']
    commands2 = ['cd /home/fasih/i200432_MLOps_A2/', 'git add data.dvc', 'git commit -m "Updated dataset"','git push origin main'] 

    os.system(' && '.join(commands))
    os.system(' && '.join(commands2))

    print("Data uploaded to dvc, pull the changes in your local repository to get the updated data.")

# if __name__ == "__main__":
#     extract()
#     transform()
#     load()

# default arguments for the DAG
default_args = {
'owner' : 'airflow-demo',
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple DAG for MLOps Assignment 2',
)

# exraction task
task1 = PythonOperator(
    task_id = "Extract_Task",
    python_callable = extract,
    dag = dag
)

# transformation task
task2 = PythonOperator(
    task_id = "Transform_Task",
    python_callable = transform,
    dag=dag
)

# loading task
task3 = PythonOperator(
    task_id = "Load_Task",
    python_callable = load,
    dag=dag
)

task1 >> task2 >> task3
