CREATE DATABASE IF NOT EXISTS `benchmark` DEFAULT CHARACTER SET utf8;

USE `benchmark`;

DROP TABLE IF EXISTS `producer`;
CREATE TABLE `producer` (
  `nr` int(11) primary key,
  `label` varchar(100) character set utf8 collate utf8_bin default NULL,
  `comment` varchar(2000) character set utf8 collate utf8_bin default NULL,
  `homepage` varchar(100) character set utf8 collate utf8_bin default NULL,
  `country` char(2) character set utf8 collate utf8_bin default NULL,
  `publisher` int(11),
  `publishDate` date
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `producer` WRITE;
ALTER TABLE `producer` DISABLE KEYS;

INSERT INTO `producer` VALUES (1,'desensitizations bleachers gentries','journalism tramroads chemoreceptive pipets wordless accumulated finches grazed packers eggbeater feebler epileptoid monadism sickened vexes mensas marimbas christianizing leary snowbound','http://www.Producer1.com/','DE',1,'2003-06-15'),(2,'decaffeinates beslime','perineum lubricator wonky lidless favouring dehorner bathless vaunty fillable ditties microcomputer underdevelopment yes basset wanning cottiers bellyaches noggins larks palettes palimpsest magnifier luxuries warworks preamplifiers melanogen sandbaggers sulphured inattentively sarcophaguses doter spattered beguines malevolently glamorizer regimenting dabbed ornamented baronetcies roots disaffiliation applications pandowdies outracing hearths yeggman tougher cockish rehashes','http://www.Producer2.com/','US',2,'2001-10-12'),(3,'paragraphing ajowans','recreancy superscription corruptibilities barbells washability marginate divagations coded bellows marathons bunkoed negativing electrum biffs mg vectors regained seeds ratiocinates audits counterfeitness sentinels highth barware dives hawaiians','http://www.Producer3.com/','KR',3,'2002-08-26');

ALTER TABLE `producer` ENABLE KEYS;
UNLOCK TABLES;