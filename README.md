# Data Engineering Project Batch_Processing 

This batch-processing data application for a data-intensive machine learning application is based on:
A team of researchers from a network of hospitals seek to investigate the average billing amount for patients admitted to hospitals within the network executed in two phases. In phase one, the focus of this project
the aim is to gain insight into how the hospitals are charging for various medical conditions. The project is executed by:
-Reading data from a of CSV file containing billing amount data the hospital issues to a patient on the date of discharge, acquired from https://www.kaggle.com/code/abhishekbhandari17/healthcare-dataset.
-Processes the data records by aggregating the average bill amount by medical condition and hospital to a monthly average.
-Incrementally loading processed data records to a database.
-Reads the processed data records, then adds a label to each record to indicate a billing amount category for each hospital in each quarter via the KMeans Machine Learning model which clusterizes the data records.
-The clusterized data records are then loaded to the database quarterly.
-For visualization the processed and clusterized data records are presented via HTML web pages.

# Project Architecture
![Project_DE_Architecture](https://github.com/user-attachments/assets/62372d66-3fb5-46a8-8707-5c38b147f1bd)



# Documentations
- The ETL Process
- ML Process
- visualization
- Are all fitted with README files detailing the execution processes.
