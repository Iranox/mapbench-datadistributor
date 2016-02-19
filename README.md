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

``-materializeMongo -join productfeatureproduct -jSource product,productfeature -jForgeinkey product,productFeature -jSourcekey nr,nr -hostMongo localhost -portMongo 27017 -databaseName bsbm``
