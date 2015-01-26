DELIMITER $$

DROP PROCEDURE IF EXISTS `sp_regiones_mapa` $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_regiones_mapa`()
BEGIN


SELECT
	t0.idregine,
	t0.regine_nombre_sd
FROM dlsmaestros.dw_ma_region_ine t0
ORDER BY t0.regine_orden;


END $$

DELIMITER ;