# bsbmloader

## init database with bsbm data 

Start import data to a MySQL database

``-parseToMongo -u root -p password  -urlMysql jdbc:mysql://localhost/benchmark ``

Important: The database in the jdbc muss exist.

## parse to MongoDB
  If a database bsm exists, it will be deleted.
``-parseToMongo -u root -p password  -urlMysql jdbc:mysql://localhost/benchmark -hostMongo localhost -portMongo 27017 -databaseName bsbm -d``


## demateriazile n:1

``-materializeMongo -target offer -source product -fk product -pk nr -hostMongo localhost -portMongo 27017 -databaseName bsbm``

## demateriazile n:m

``-materializeMongo -hostMongo localhost -portMongo 27017 -databaseName bsbm -source product -fk productFeature -join productfeatureproduct -secondSource productfeature -pkSecond nr -pk nr -secondFkey product``

## parse to CouchDB
``-parseToMongo -u root -p password  -urlMysql jdbc:mysql://localhost/benchmark -hostMongo localhost -portMongo 27017 -databaseName bsbm``

## get a row (CouchDB)
To get all ids
``curl -X GET http://username:password@host:port/table/_all_docs``
To get a value
``curl -X GET http://username:password@host:port/table/id``

