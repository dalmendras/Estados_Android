DELIMITER $$

DROP PROCEDURE IF EXISTS `sp_comuna_mapa` $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_comuna_mapa`()
BEGIN


SELECT
	t4.idregine,
	t4.regine_nombre_sd,
	t2.idcomuine,
	t2.comuine_nombre,
	t1.agencodigo,
	t1.agendescripcion,
	t0.agenlat,
	t0.agenlon,
	t0.agendireccion,
	t0.agenhorariosem,
	t0.agenhorariosab,
	t0.agenhorariodom,
	t0.agenhorariofest,
	t0.agentelefono,
	t0.agenmail,
	t0.agenservcel,
	t0.agenservgiro,
	t0.agenservpasaje,
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
FROM dlsmaestros.dw_ma_agencia_xy t0
INNER JOIN dlsmaestros.ma_agencia       t1 ON t1.agencodigo   = t0.agencodigo
INNER JOIN dlsmaestros.dw_ma_comuna_ine    t2 ON t2.idcomuine    = t0.idcomuine
INNER JOIN dlsmaestros.dw_ma_provincia_ine t3 ON t3.idprovine = t2.idprovine
INNER JOIN dlsmaestros.dw_ma_region_ine    t4 ON t4.idregine    = t3.idregine
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

WHERE t0.agenpubversion = 1
ORDER BY t2.idcomuine ASC;



END $$

DELIMITER ;