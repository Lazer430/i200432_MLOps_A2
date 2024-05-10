## Fasih Abdullah i200432 DS-N A2 Report

### Details


This report dicusses the steps performed to create and apache airflow dag data pipeline and its associated challenges.

The steps are described as follows:

1) Data Extraction Phase:
This node uses the sources list and fetches all of links on the homepage of each source site. Then the node extracts all of the titles and descriptions from the top 20 (first) sites. The limit of 20 sites was used to ensure quick run time at demo.

![Screenshot 2024-05-10 194641](https://github.com/Lazer430/i200432_MLOps_A2/assets/90345992/67ec31b6-ed98-4b2f-af44-d524b8de9fdb)

3) Transformation Phase:
In this phase the extracted data is recieved from the extract node and preprocesing is done on the data. This inlcudes removal of non ascii characters, removal of endlines, carriage returns, and tab spaces. Then the titles and descriptions were filtered to rempve emmpty entries and then all of the text was converted to lowercase

![Screenshot 2024-05-10 194654](https://github.com/Lazer430/i200432_MLOps_A2/assets/90345992/e989d807-e049-4b58-abea-31a1d937a748)

4) Loading Phase:
In this phase, the preprocessed data is recieved from the tranformation node and is converted to a local csv file. This csv file is then versioned using dvc and the dataset meta data is pushed to the github repo.

![Screenshot 2024-05-10 194709](https://github.com/Lazer430/i200432_MLOps_A2/assets/90345992/7a263737-d5f6-4d21-85df-b825a695ca28)

### Challenges

During implementation, the following challenges were faced:

1) Apache airflow was not detecting the dag from my repo. This was corrected by changing the airflow.conf file such that the dag directory was set to the repo directory.

2) The steps are executed on separate runners that are isolated from one another. So there was the challenge of how to pass the datset across the nodes. Upon research, there were two ways xcom and airflow.models.Variable. The latter was more suited for this situation and resolved the issue. 
