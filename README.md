# bsbmloader

## init database with bsbm data 

Start import data to a MySQL database

``-parseToMongo -u root -p password  -urlMySQL jdbc:mysql://localhost/benchmark ``

Important: The database in the jdbc muss exist.

## parse to MongoDB

``-parseToMongo -u root -p password  -urlMySQL jdbc:mysql://localhost/benchmark -hostMongo localhost -portMongo 27017 ``
