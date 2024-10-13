# Visualization of the project via HTML

The results of the application can be visualized via `localhost:8080` while the application is running.

The pages are updated at 120 second interval.

## Home Page

From the home page, the tables can be accessed by clicking the links indicating the name of the page's application.

- The ETL Process displays the table generated from the ETL execution.
- The Machine Learning Process displays the table created by the ML execution which is the application of clusterization following the ETL process.
depending on the billing amount issued by the hospital on the discharge date.

## Process Status

This page gives an overview of project's execution displaying status information on the execution of each application.

Note: the "Last Date Loaded" column displays the date when the last record was loaded to the database, thus subsequent executions are only ran on new data. 
Given the pages are updated at 120 seconds intervals, this should be taken into account when displaying the results

Therefore; this page facilitates the monitoring and recording of ETL and ML execution processes.
- After the status `ETL 7/7` is displayed, the ETL Process page will display the ETL results*
- After the status `ML 2/2` is displayed, the Machine Learning Process page will display the ML results*


In a nutshell the billing amount is aggregated by hospital and the "Bill Cat" column is the name of the cluster assigned to bill the hospital issues on the discharge date.