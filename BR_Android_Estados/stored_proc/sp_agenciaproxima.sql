DELIMITER $$

DROP PROCEDURE IF EXISTS `sp_agenciaproxima` $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_agenciaproxima`(IN `p_lat` DECIMAL(10,7), IN `p_lon` DECIMAL(10,7), IN `radio` INT)
BEGIN



SELECT
	t2.ciudregion,
	t0.agencodigo,
	t0.agendescripcion,
	t6.comuine_nombre,
	t1.agenlat,
	t1.agenlon,
	t1.agendireccion,
	t1.agenhorariosem,
	t1.agenhorariosab,
	t1.agenhorariodom,
	t1.agenhorariofest,
	t1.agentelefono,
	t1.agenmail,
	t1.agenservcel,
	t1.agenservgiro,
	t1.agenservpasaje,
	t6.MT_A_N,
	t6.BT_A_N,
	t6.GT_A_N,
	t6.MT_A_E,
	t6.BT_A_E,
	t6.GT_A_E,
	t6.MT_D_N,
	t6.BT_D_N,
	t6.GT_D_N,
	t6.MT_D_E,
	t6.BT_D_E,
	t6.GT_D_E,
	CASE WHEN 
	t6.MT_A_N IS NULL AND
	t6.BT_A_N IS NULL AND
	t6.GT_A_N IS NULL AND
	t6.MT_A_E IS NULL AND
	t6.BT_A_E IS NULL AND
	t6.GT_A_E IS NULL AND
	t6.MT_D_N IS NULL AND
	t6.BT_D_N IS NULL AND
	t6.GT_D_N IS NULL AND
	t6.MT_D_E IS NULL AND
	t6.BT_D_E IS NULL AND
	t6.GT_D_E IS NULL 
	THEN false
	ELSE true
	END AS rest_ent
FROM dlsmaestros.ma_agencia t0
	INNER JOIN dlsmaestros.dw_ma_agencia_xy t1 ON t1.agencodigo = t0.agencodigo
	INNER JOIN dlsmaestros.dw_ma_comuna_ine    t6 ON t6.idcomuine    = t1.idcomuine
	INNER JOIN dlsmaestros.ma_ciudad        t2 ON t2.ciudcodigo = t0.ciudcodigo
	INNER JOIN dlsmaestros.ma_region        t3 ON t3.regcodigo  = t2.ciudregion
	 LEFT JOIN (SELECT t5.agencodigo,
				SUM(IF(t5.aresclasificacion = 'MT' and t5.tentcodigo = 1 and t5.tsercodigo = 0, 1, null)) as 'MT_A_N',
				SUM(IF(t5.aresclasificacion = 'BT' and t5.tentcodigo = 1 and t5.tsercodigo = 0, 1, null)) as 'BT_A_N',
				SUM(IF(t5.aresclasificacion = 'GT' and t5.tentcodigo = 1 and t5.tsercodigo = 0, 1, null)) as 'GT_A_N',
				SUM(IF(t5.aresclasificacion = 'MT' and t5.tentcodigo = 1 and t5.tsercodigo = 1, 1, null)) as 'MT_A_E',
				SUM(IF(t5.aresclasificacion = 'BT' and t5.tentcodigo = 1 and t5.tsercodigo = 1, 1, null)) as 'BT_A_E',
				SUM(IF(t5.aresclasificacion = 'GT' and t5.tentcodigo = 1 and t5.tsercodigo = 1, 1, null)) as 'GT_A_E',
				SUM(IF(t5.aresclasificacion = 'MT' and t5.tentcodigo = 2 and t5.tsercodigo = 0, 1, null)) as 'MT_D_N',
				SUM(IF(t5.aresclasificacion = 'BT' and t5.tentcodigo = 2 and t5.tsercodigo = 0, 1, null)) as 'BT_D_N',
				SUM(IF(t5.aresclasificacion = 'GT' and t5.tentcodigo = 2 and t5.tsercodigo = 0, 1, null)) as 'GT_D_N',
				SUM(IF(t5.aresclasificacion = 'MT' and t5.tentcodigo = 2 and t5.tsercodigo = 1, 1, null)) as 'MT_D_E',
				SUM(IF(t5.aresclasificacion = 'BT' and t5.tentcodigo = 2 and t5.tsercodigo = 1, 1, null)) as 'BT_D_E',
				SUM(IF(t5.aresclasificacion = 'GT' and t5.tentcodigo = 2 and t5.tsercodigo = 1, 1, null)) as 'GT_D_E'
			FROM dlsmaestros.ma_restric_destino t5
			WHERE t5.aresversion = 1
			AND t5.aresisdisponible = 1
			GROUP BY t5.agencodigo ) AS t6 ON t6.agencodigo = t0.agencodigo
WHERE t1.agenpubversion = 1
  AND android.GetDistance(p_lat, p_lon, t1.agenlat, t1.agenlon) <= radio;



END $$

DELIMITER ;