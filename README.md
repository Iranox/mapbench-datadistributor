# bsbmloader

## init a MySQL database with bsbm data

Start import data to a MySQL database

``-importToMysql -u root -p password  -urlMysql jdbc:mysql://localhost/benchmark ``

Important: The database in the jdbc muss exist.

## MongoDB
### parse to MongoDB
  If a database bsm exists, it will be deleted.   
``-parseToMongo -u root -p password  -urlMysql jdbc:mysql://localhost/benchmark -hostMongo localhost -portMongo 27017 -databaseName bsbm -d``  

  Without -d the database will still exist.

### demateriazile n:1

``-materializeMongo -target offer -source product -fk product -pk nr -hostMongo localhost -portMongo 27017 -databaseName bsbm``

### demateriazile n:m

``-materializeMongo -hostMongo localhost -portMongo 27017 -databaseName bsbm -source product -fk productFeature -join productfeatureproduct -secondSource productfeature -pkSecond nr -pk nr -secondFkey product``

## CouchDB
### parse to CouchDB
``-parseToMongo -u root -p password  -urlMysql jdbc:mysql://localhost/benchmark -hostMongo localhost -portMongo 27017 -databaseName bsbm``

### demateriazile n:1

``-materializeCouch -target offer -source product -fk product -pk nr -hostNosql localhost -portNosql 5986 -databaseName bsbm -userCouch admin  -passwordCouch password``

### demateriazile n:m  
``-materializeCouch -hostNosql localhost -portNosql 5986 -databaseName bsbm -source product -fk productFeature -join productfeatureproduct -secondSource productfeature -pkSecond nr -pk nr -secondFkey product -userCouch admin  -passwordCouch password``

### get a row (CouchDB)
To get all ids
``curl -X GET http://username:password@host:port/table/_all_docs``
To get a value
``curl -X GET http://username:password@host:port/table/id``

## Excel
### parse to Excel

The excel file must exist.  
``-parseToExcel -u root -p password -urlMysql jdbc:mysql://localhost/benchmark -excelFile  C:\Users\username\Desktop\Test.xlsx``

At the moment, bsbmloader does not support demateriazile an excel file.

## ElasticSearch
### parte to ElasticSearch   

``-parseToElastic -u root -p password -urlMysql jdbc:mysql://localhost/benchmark -hostNosql localhost -portNosql 27017 -databaseName bsbm``

### Get 1000 rows 
http://127.0.0.1:9200/bla/_search/?size=1000&pretty=1



