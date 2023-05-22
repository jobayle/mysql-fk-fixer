-- MySQL dump 10.13  Distrib 5.7.40, for Linux (x86_64)
--
-- Host: localhost    Database: my_schema
-- ------------------------------------------------------
-- Server version	5.7.40

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bar`
--

DROP TABLE IF EXISTS `bar`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bar` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `info` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `bar`
--

LOCK TABLES `bar` WRITE;
/*!40000 ALTER TABLE `bar` DISABLE KEYS */;
INSERT INTO `bar` VALUES (1,'hello'),(2,'world');
/*!40000 ALTER TABLE `bar` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `baz`
--

DROP TABLE IF EXISTS `baz`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `baz` (
  `foo_id` bigint(20) NOT NULL,
  `bar_id` bigint(20) NOT NULL,
  KEY `foo_id` (`foo_id`),
  KEY `bar_id` (`bar_id`),
  CONSTRAINT `baz_ibfk_1` FOREIGN KEY (`foo_id`) REFERENCES `foo` (`id`),
  CONSTRAINT `baz_ibfk_2` FOREIGN KEY (`bar_id`) REFERENCES `bar` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `baz`
--

LOCK TABLES `baz` WRITE;
/*!40000 ALTER TABLE `baz` DISABLE KEYS */;
INSERT INTO `baz` VALUES (1,1),(2,2),(3,3);
/*!40000 ALTER TABLE `baz` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `foo`
--

DROP TABLE IF EXISTS `foo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `foo` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `val` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `foo`
--

LOCK TABLES `foo` WRITE;
/*!40000 ALTER TABLE `foo` DISABLE KEYS */;
INSERT INTO `foo` VALUES (1,'foo'),(2,'bar'),(3,'baz');
/*!40000 ALTER TABLE `foo` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-05-16 10:03:28
