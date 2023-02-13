# Interview project
This is the test required for the hiring process,

## Description

The test has been approached by using a neo4j database to store the graph of nodes and the data processing and api exposing has been done in Python. 

You can find the following endpoints:

* http://localhost:5000/api/top_n
* http://localhost:5000/api/find_shortest/path_weight
* http://localhost:5000/api/find_shortest/num_nodes

These are documented in the index.html file inside the documentation folder. You should be able to open it with any browser.

Here are some sample requests:
* curl -X GET 'http://localhost:5000/api/top_n?n=10&cluster=c0'
* curl -X GET 'http://localhost:5000/api/find_shortest/path_weight?JobId1=e7d46871bd71370d95cf9da763c8e634&JobId2=96b18d4f35068e69a4ba7e1e976643a5'
* curl -X GET 'http://localhost:5000/api/find_shortest/num_nodes?JobId1=e7d46871bd71370d95cf9da763c8e634&JobId2=96b18d4f35068e69a4ba7e1e976643a5' 

### Installing

Make sure you have docker with docker compose.

* clone this repository

### Executing program

* ```cd``` into the folder of the repository
* Run ```docker compose up```
* Wait until the database is up and the data has been loaded (the database container takes about 30 seconds to spin up)
* make requests to the endpoints

## Help

The data may take some time to load. As I do not have the best machine, I had to load the data in batches of 5000 transactions. You may edit the batch size to increase performance.

## Authors

Sergi Duaigües