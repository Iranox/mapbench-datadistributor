CREATE DATABASE IF NOT EXISTS `benchmark` DEFAULT CHARACTER SET utf8;

USE `benchmark`;

DROP TABLE IF EXISTS `vendor`;
CREATE TABLE `vendor` (
  `nr` int(11) primary key,
  `label` varchar(100) character set utf8 collate utf8_bin default NULL,
  `comment` varchar(2000) character set utf8 collate utf8_bin default NULL,
  `homepage` varchar(100) character set utf8 collate utf8_bin default NULL,
  `country` char(2) character set utf8 collate utf8_bin default NULL,
  `publisher` int(11),
  `publishDate` date
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `vendor` WRITE;
ALTER TABLE `vendor` DISABLE KEYS;

INSERT INTO `vendor` VALUES (1,'outreasons','canebrake tailored noncivilized teuton vined adsorptively electrocardiographs subbing mitigator squarest phosgenes gallinules collops redesigned doings purposing nictated birthmarks displayed chemical cottiers whoopee provocatively luffs accedence aliening ombudsmen','http://www.vendor1.com/','GB',1,'2008-05-31');

ALTER TABLE `vendor` ENABLE KEYS;
UNLOCK TABLES;