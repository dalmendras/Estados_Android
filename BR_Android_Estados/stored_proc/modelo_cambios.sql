-- original v.1.0
/*
CREATE TABLE  `android`.`mv_of_estado` (
  `odflcodigo` int(11) NOT NULL,
  `odflfechemicorta` date NOT NULL,
  `eprocodigo` smallint(6) NOT NULL,
  `epropd` tinyint(3) unsigned NOT NULL,
  `epropda` tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`odflcodigo`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE  `android`.`mv_of_estado_temp` (
  `odflcodigo` int(11) NOT NULL,
  `odflfechemicorta` date NOT NULL,
  `eprocodigo` smallint(6) NOT NULL,
  `epropd` tinyint(3) unsigned NOT NULL,
  `epropda` tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`odflcodigo`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
*/


-- cambios version v.1.0 a v.1.2

ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `odflfechentrest` date DEFAULT NULL AFTER `epropda`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `odflfechentrega` date DEFAULT NULL AFTER `odflfechentrest`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `tsercodigo` smallint(6) unsigned DEFAULT NULL AFTER `odflfechentrega`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `tentcodigo` smallint(6) unsigned DEFAULT NULL AFTER `tsercodigo`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `tpagcodigo` smallint(6) unsigned DEFAULT NULL AFTER `tentcodigo`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `ciudcodigoorigen` int(11) unsigned DEFAULT NULL AFTER `tpagcodigo`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `ciudcodigodestino` int(11) unsigned DEFAULT NULL AFTER `ciudcodigoorigen`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `agencodigoorigen` int(11) unsigned DEFAULT NULL AFTER `ciudcodigodestino`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `agencodigodestino` int(11) unsigned DEFAULT NULL AFTER `agencodigoorigen`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `comucodigo` smallint(6) DEFAULT NULL AFTER `agencodigodestino`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `entrut` int(11) DEFAULT NULL AFTER `comucodigo`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `entdv` CHAR(1) DEFAULT NULL AFTER `entrut`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `entapellidop` VARCHAR(20) DEFAULT NULL AFTER `entdv`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `entapellidom` VARCHAR(20) DEFAULT NULL AFTER `entapellidop`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `entnombres` VARCHAR(20) DEFAULT NULL AFTER `entapellidom`;
ALTER TABLE `android`.`mv_of_estado` ADD COLUMN `entfecha` date DEFAULT NULL AFTER `entnombres`;

ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `odflfechentrest` date DEFAULT NULL AFTER `epropda`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `odflfechentrega` date DEFAULT NULL AFTER `odflfechentrest`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `tsercodigo` smallint(6) unsigned DEFAULT NULL AFTER `odflfechentrega`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `tentcodigo` smallint(6) unsigned DEFAULT NULL AFTER `tsercodigo`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `tpagcodigo` smallint(6) unsigned DEFAULT NULL AFTER `tentcodigo`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `ciudcodigoorigen` int(11) unsigned DEFAULT NULL AFTER `tpagcodigo`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `ciudcodigodestino` int(11) unsigned DEFAULT NULL AFTER `ciudcodigoorigen`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `agencodigoorigen` int(11) unsigned DEFAULT NULL AFTER `ciudcodigodestino`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `agencodigodestino` int(11) unsigned DEFAULT NULL AFTER `agencodigoorigen`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `comucodigo` smallint(6) DEFAULT NULL AFTER `agencodigodestino`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `entrut` int(11) DEFAULT NULL AFTER `comucodigo`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `entdv` CHAR(1) DEFAULT NULL AFTER `entrut`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `entapellidop` VARCHAR(20) DEFAULT NULL AFTER `entdv`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `entapellidom` VARCHAR(20) DEFAULT NULL AFTER `entapellidop`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `entnombres` VARCHAR(20) DEFAULT NULL AFTER `entapellidom`;
ALTER TABLE `android`.`mv_of_estado_temp` ADD COLUMN `entfecha` date DEFAULT NULL AFTER `entnombres`;