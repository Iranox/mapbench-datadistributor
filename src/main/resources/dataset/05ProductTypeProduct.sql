CREATE DATABASE IF NOT EXISTS `benchmark` DEFAULT CHARACTER SET utf8;

USE `benchmark`;

DROP TABLE IF EXISTS `producttypeproduct`;
CREATE TABLE `producttypeproduct` (
  `product` int(11) not null,
  `productType` int(11) not null,
  PRIMARY KEY (product, productType)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `producttypeproduct` WRITE;
ALTER TABLE `producttypeproduct` DISABLE KEYS;

INSERT INTO `producttypeproduct` VALUES (1,11),(2,15),(3,11),(4,15),(5,10),(6,17),(7,14),(8,16),(9,9),(10,11),(11,6),(12,6),(13,6),(14,16),(15,10),(16,7),(17,21),(18,6),(19,6),(20,7),(21,21),(22,16),(23,14),(24,18),(25,15),(26,8),(27,16),(28,18),(29,9),(30,9),(31,21),(32,17),(33,16),(34,8),(35,17),(36,18),(37,12),(38,10),(39,7),(40,7),(41,12),(42,13),(43,17),(44,17),(45,7),(46,8),(47,18),(48,11),(49,10),(50,8),(51,9),(52,8),(53,7),(54,8),(55,6),(56,9),(57,10),(58,12),(59,10),(60,12),(61,7),(62,7),(63,13),(64,7),(65,10),(66,13),(67,13),(68,7),(69,13),(70,15),(71,11),(72,8),(73,11),(74,7),(75,11),(76,18),(77,17),(78,7),(79,10),(80,7),(81,10),(82,15),(83,7),(84,6),(85,12),(86,9),(87,12),(88,14),(89,16),(90,13),(91,18),(92,13),(93,18),(94,17),(95,6),(96,10),(97,18),(98,8),(99,8),(100,9);

ALTER TABLE `producttypeproduct` ENABLE KEYS;
UNLOCK TABLES;