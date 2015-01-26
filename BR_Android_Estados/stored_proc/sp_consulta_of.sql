DELIMITER $$

DROP PROCEDURE IF EXISTS `sp_consulta_of` $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_consulta_of`(in codigo char(30))
BEGIN

SELECT 
	proc.eprodescripcion as estado
FROM  mv_of_estado of
INNER JOIN dlsmaestros.ma_estado_proceso proc ON proc.eprocodigo = of.eprocodigo
WHERE of.odflcodigo = codigo;


END $$

DELIMITER ;