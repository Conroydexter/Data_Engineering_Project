# Machine Learning Computations

The main objective of the ML process is to perform clusterization on the data from the ETL process, then categories each hospital per quarter based on the bill amount issued to an admitted patient on the discharge date. 

There are three categories that a bill amount can be assigned, Low, Medium, or High. 


Assigning each cluster is accomplished by averaging the billing amount data for a given hospital aggregated by quarter.
The process is carried out based on an algorithm which re-assign and re-load the data every quarter to ensure the most up-to-date cluster distribution.

## Execution

-The ETL process is first executed
-The Machine Learning computation is executed after the ETL process is accomplished.

NOTE the status table is used for the verification of the execution steps.

## Schedule

The execution schedule is dependent on whether it is in Test Mode or Production Mode.
- Test Mode = Process executed every 15 min
- Production Mode =  Executed quarterly

The execution mode can be toggled by changing the `EXECUTION_MODE` variable under `ml` in the [docker-compose.yaml](../../docker-compose.yaml) file

### Test Mode:
```
environment:
    - EXECUTION_MODE=test
```

### Production Mode:
```
environment:
    - EXECUTION_MODE=production