import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import tqdm
from airflow.models import Variable
import warnings

sources = ['https://www.dawn.com/']
links = [[] for i in range(len(sources))]
links_dataframe = None

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

    print(links_dataframe.head())

    Variable.set("links_dataframe", links_dataframe.to_json(), serialize_json=True)

    


def transform(**kwargs):

    links_dataframe = Variable.get("links_dataframe", deserialize_json=True)

    if type(links_dataframe) != pd.DataFrame:
        links_dataframe = pd.read_json(links_dataframe)

    print("Transformation")

    # clean the links dataframe to remove any empty links
    links_dataframe = links_dataframe[links_dataframe['Links'].apply(lambda x: len(x) > 0)]
    # remove any duplicate links
    links_dataframe['Links'] = links_dataframe['Links'].apply(lambda x: list(set(x)))

    # remove incomplete links
    links_dataframe['Links'] = links_dataframe['Links'].apply(lambda x: [link for link in x if link.startswith('http')])

    print(links_dataframe.head())

    # extract titles and descriptions of the links
    data = pd.DataFrame(columns=['Link', 'Title', 'Description'])
    for i, source in enumerate(sources):
        #for link in links_dataframe.loc[i, 'Links']:
        for link in tqdm.tqdm(links_dataframe.loc[i, 'Links']):
            reqs = requests.get(link)
            if reqs.status_code != 200:
                continue
            soup = BeautifulSoup(reqs.text, 'html.parser')
            title = soup.find('title')
            title = title.get_text() if title else ''
            if title:
                title = title.encode('ascii', 'ignore').decode('ascii')
            else:
                continue
            description = " ".join([p.get_text() for p in soup.find_all('p')])
            if description == '' or description == ' ':
                continue

            # preprocess description to remove symbols
            description = description.encode('ascii', 'ignore').decode('ascii')
            description = description.replace('\n', ' ')
            description = description.replace('\r', ' ')
            data = data._append({'Link': link, 'Title': title, 'Description': description}, ignore_index=True)

    links_dataframe = data

    print(data.head())

    Variable.set("links_dataframe", links_dataframe.to_json(), serialize_json=True)

def load(**kwargs):

    links_dataframe = Variable.get("links_dataframe", deserialize_json=True)

    if type(links_dataframe) != pd.DataFrame:
        links_dataframe = pd.read_json(links_dataframe)

    print("Loading")

    # print(os.getcwd())    

    # if not os.path.exists('./data'):
    #     os.makedirs('./data')

    links_dataframe.to_csv('/home/fasih/i200432_MLOps_A2/data/links.csv', index=False)
    print("Data saved to links.csv")

    commands = ['cd /home/fasih/i200432_MLOps_A2/', 'dvc add ./data/links.csv', 'git add data.dvc', 'git commit -m "Updated dataset"', 'git push origin main', 'dvc push']

    os.system(' && '.join(commands))

    print("Data uploaded to dvc, pull the changes in your local repository to get the updated data.")



# if __name__ == "__main__":
    # define apache airflow pipeline

default_args = {
'owner' : 'airflow-demo',
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple '
)

# with dag:
task1 = PythonOperator(
    task_id = "Extract_Task",
    python_callable = extract,
    dag = dag
)

task2 = PythonOperator(
    task_id = "Transform_Task",
    python_callable = transform,
    dag=dag
)

task3 = PythonOperator(
    task_id = "Load_Task",
    python_callable = load,
    dag=dag
)

task1 >> task2 >> task3
