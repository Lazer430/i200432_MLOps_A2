# Fasih Abdullah i200432 DS-N A2 MLOps 
# Apache Airflow ETL Data pipeline

## Description of Assignment Task
The aim of this assignment was to construct and automate an ETL data pipline with the help of apache airflow.
The instructions were to scan a list of sources and extract the hyperlinks from each home page and use these to extract the titles and descriptions of each article.
Then the extracted data would be transformed by application of data preprocessing to clean and regularize the data to a more manageable and meaningful form.
FInally the transformed data would be loaded onto a remote google drive and the dataset would be versioned using dvc and its meta data using git.

## Steps performed
1) Extraction of data using web scraping methods
2) Data preprocessing using python
3) CSV Dataset generation and DVC versioning

## Layout of files
- Data folder contains the csv dataset but is not uploaded to the repo as it defeats the purpose of using dvc.
- data.dvc is used instead for dvc versioning and dataset can be pulled using `dvc pull`
- main.py contains all of the dag code
- .dvc folder has dvc configuration files
- Report.md outlines the steps of the assignment with screenshots
- Data preprocessing steps and dvc setup constains information about the preprocessing perfomed and the steps for setting up dvc for dataset versioning.
