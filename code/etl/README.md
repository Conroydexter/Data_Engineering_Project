# Objective and execution of the ETL pipeline

The core mission is the extraction of data from the source(csv file), perform sequential transformations on the data in preparation for the ML executions to be carried out, and finally the data is loaded into a postgre database.

 The bill amount issued on discharge date for patients admitted to hospitals in the network are placed into  monthly averages via aggregation.

- A new column named quarter is added for the quarterly breakdown when conducting the ML executions.

- Several column that are not necessary for the investigation is dropped.

- The average billing amount is grouped by the columns Medical Condition, Hospital, Insurance Provider, Admission Type, and Discharge Date.

To check the last uploaded data, the Status table is employed. In addition, the database is loaded only with new data.
The data in the table is deleted before a new full load (refreshed), to accomodate any failures that my result in an incomplete load. 


## Schedule

The execution schedule is  dependent on whether it is in Test Mode or Production Mode.
- Test Mode = execution every 15 min
- Production Mode = execution every month

The execution mode can be toggled by changing the `EXECUTION_MODE` variable under `etl` in the [docker-compose.yaml](../../docker-compose.yaml) file

### Test Mode:
```
environment:
    - EXECUTION_MODE=test
```

### Production Mode:
```
environment:
    - EXECUTION_MODE=production
```