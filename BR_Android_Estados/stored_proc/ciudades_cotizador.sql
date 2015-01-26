DELIMITER $$

DROP PROCEDURE IF EXISTS `ciudades_cotizador` $$
CREATE DEFINER=`root`@`%` PROCEDURE `ciudades_cotizador`()
BEGIN

SELECT *
FROM
	(SELECT DISTINCT 'ORIGEN' AS punto, t3.regcodigo, 
	t0.ciudcodigo, t0.ciudnombre
	FROM dlsmaestros.ma_ciudad t0
	INNER JOIN dlsmaestros.ma_agencia       t1 ON t1.ciudcodigo = t0.ciudcodigo
	INNER JOIN dlsmaestros.rl_ag_linea_prod t2 ON t2.agencodigo = t1.agencodigo
	INNER JOIN dlsmaestros.ma_region        t3 ON t3.regcodigo = t0.ciudregion
	WHERE
		    t0.ciudcodigo != 0
		AND t0.ciudversion = 1
		AND t1.emprcodigo  = 1     
		AND t1.agenversion = 1
		AND t2.linecodigo  = 1     
		AND t2.raliemite   = 1
		AND t2.raliversion = 1
		AND EXISTS (SELECT 1
		            FROM dlsmaestros.ma_ubicacion_fisic t4
		            WHERE t4.agencodigo  = t1.agencodigo
		              AND t4.tubicodigo  = 1                
		              AND t4.ubifversion = 1)
	ORDER BY t3.regorden ASC, t0.ciudnombre ASC ) as origen
	UNION ALL
	
	SELECT *
	FROM
		(
		SELECT DISTINCT dest.punto, dest.regcodigo, dest.ciudcodigo, dest.ciudnombre
		FROM
			(
			SELECT DISTINCT 'DESTINO' AS punto, t3.regcodigo, t3.regorden,
			t0.ciudcodigo, t0.ciudnombre
			FROM dlsmaestros.ma_ciudad t0
			INNER JOIN dlsmaestros.ma_agencia       t1 ON t1.ciudcodigo = t0.ciudcodigo
			INNER JOIN dlsmaestros.rl_ag_linea_prod t2 ON t2.agencodigo = t1.agencodigo
			INNER JOIN dlsmaestros.ma_region        t3 ON t3.regcodigo  = t0.ciudregion
			WHERE
				    t0.ciudcodigo  != 0
				AND t0.ciudversion  = 1
				AND t1.emprcodigo   = 1       
				AND t1.agenversion  = 1
				AND t2.linecodigo   = 1       
				
				AND t2.raliemite    = 1
				AND t2.raliversion  = 1
				AND EXISTS (SELECT 1
				            FROM dlsmaestros.ma_ubicacion_fisic t4
				            INNER JOIN dlsmaestros.ma_restric_destino t5 ON t5.agencodigo = t4.agencodigo
				            WHERE t4.agencodigo       = t1.agencodigo
				              AND t4.tubicodigo       = 1                
				              AND t4.ubifversion      = 1
				              AND t5.aresisdisponible = 1
				              AND t5.aresversion      = 1)
			UNION ALL
			SELECT DISTINCT 'DESTINO' AS punto, t9.regcodigo, t9.regorden,
			t5.ciudcodigo, t8.ciudnombre
			FROM dlsmaestros.ma_dest_indirecto t5
			INNER JOIN dlsmaestros.ma_agencia       t6 ON t6.agencodigo = t5.agencodigobase
			INNER JOIN dlsmaestros.rl_ag_linea_prod t7 ON t7.agencodigo = t5.agencodigobase
			INNER JOIN dlsmaestros.ma_ciudad        t8 ON t8.ciudcodigo = t5.ciudcodigo
			INNER JOIN dlsmaestros.ma_region        t9 ON t9.regcodigo  = t8.ciudregion
			WHERE
			        t5.deinversion     = 1
			    AND t6.emprcodigo      = 1     
				AND t6.agenversion     = 1
				AND t7.linecodigo      = 1     
				
				AND t7.raliversion     = 1
				AND t8.ciudversion     = 1
				AND EXISTS (SELECT 1
				            FROM dlsmaestros.ma_ubicacion_fisic t10
				            INNER JOIN dlsmaestros.ma_restric_destino t11 ON t11.agencodigo = t10.agencodigo
				            WHERE t10.agencodigo       = t6.agencodigo
				              AND t10.tubicodigo       = 1                
				              AND t10.ubifversion      = 1
				              AND t11.aresisdisponible = 1
				              AND t11.aresversion      = 1)
			) AS dest
		ORDER BY dest.regorden ASC, dest.ciudnombre ASC) AS destino;


END $$

DELIMITER ;