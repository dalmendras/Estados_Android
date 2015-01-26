DELIMITER $$

DROP PROCEDURE IF EXISTS `sp_estado_of_movil` $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_estado_of_movil`()
BEGIN

-- Dump Insert Cloud --
SELECT
	t0.*
FROM mv_of_estado_temp t0
  LEFT JOIN mv_of_estado t1 ON t0.odflcodigo = t1.odflcodigo
WHERE t1.odflcodigo IS NULL

INTO OUTFILE '/home/procesos_carga/act_estado_movil/mv_of_estado_insert.txt'
-- INTO OUTFILE pathFileInsert
FIELDS TERMINATED BY '\t'
-- OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- Dump Update Cloud --

SELECT
concat(
	'update mv_of_estado set ',
	concat_ws(',',
			if(t0.eprocodigo != t1.eprocodigo or (t0.eprocodigo is null and t1.eprocodigo is not null), concat('eprocodigo =', t1.eprocodigo), null),
			if(t0.epropd     != t1.epropd     or (t0.epropd     is null and t1.epropd     is not null), concat('epropd =',     t1.epropd), null),
			if(t0.epropda    != t1.epropda    or (t0.epropda    is null and t1.epropda    is not null), concat('epropda =',    t1.epropda), null),

      if(t0.odflfechentrest   != t1.odflfechentrest   or (t0.odflfechentrest   is null and t1.odflfechentrest   is not null), concat("odflfechentrest ='", t1.odflfechentrest,"'"), null),
      if(t0.odflfechentrega   != t1.odflfechentrega   or (t0.odflfechentrega   is null and t1.odflfechentrega   is not null), concat("odflfechentrega ='", t1.odflfechentrega,"'"), null),
      if(t0.tsercodigo        != t1.tsercodigo        or (t0.tsercodigo        is null and t1.tsercodigo        is not null), concat('tsercodigo =',       t1.tsercodigo), null),
      if(t0.tentcodigo        != t1.tentcodigo        or (t0.tentcodigo        is null and t1.tentcodigo        is not null), concat('tentcodigo =',       t1.tentcodigo), null),
      if(t0.tpagcodigo        != t1.tpagcodigo        or (t0.tpagcodigo        is null and t1.tpagcodigo        is not null), concat('tpagcodigo =',       t1.tpagcodigo), null),
      if(t0.ciudcodigoorigen  != t1.ciudcodigoorigen  or (t0.ciudcodigoorigen  is null and t1.ciudcodigoorigen  is not null), concat('ciudcodigoorigen =', t1.ciudcodigoorigen), null),
      if(t0.ciudcodigodestino != t1.ciudcodigodestino or (t0.ciudcodigodestino is null and t1.ciudcodigodestino is not null), concat('ciudcodigodestino =',t1.ciudcodigodestino), null),
      if(t0.agencodigoorigen  != t1.agencodigoorigen  or (t0.agencodigoorigen  is null and t1.agencodigoorigen  is not null), concat('agencodigoorigen =', t1.agencodigoorigen), null),
      if(t0.agencodigodestino != t1.agencodigodestino or (t0.agencodigodestino is null and t1.agencodigodestino is not null), concat('agencodigodestino =',t1.agencodigodestino), null),
      if(t0.comucodigo        != t1.comucodigo        or (t0.comucodigo        is null and t1.comucodigo        is not null), concat('comucodigo =',       t1.comucodigo), null),
      if(t0.entrut            != t1.entrut            or (t0.entrut            is null and t1.entrut            is not null), concat('entrut =',           t1.entrut), null),
      if(t0.entdv             != t1.entdv             or (t0.entdv             is null and t1.entdv             is not null), concat("entdv ='",           t1.entdv,"'"), null),
      if(t0.entapellidop      != t1.entapellidop      or (t0.entapellidop      is null and t1.entapellidop      is not null), concat("entapellidop ='",    t1.entapellidop,"'"), null),
      if(t0.entapellidom      != t1.entapellidom      or (t0.entapellidom      is null and t1.entapellidom      is not null), concat("entapellidom ='",    t1.entapellidom,"'"), null),
      if(t0.entnombres        != t1.entnombres        or (t0.entnombres        is null and t1.entnombres        is not null), concat("entnombres ='",      t1.entnombres,"'"), null),
      if(t0.entfecha          != t1.entfecha          or (t0.entfecha          is null and t1.entfecha          is not null), concat("entfecha ='",        t1.entfecha,"'"), null)

			),
	' where odflcodigo = ',
	t1.odflcodigo,
	';'
	)
FROM mv_of_estado t0
INNER JOIN mv_of_estado_temp t1 on t1.odflcodigo = t0.odflcodigo
WHERE (t0.eprocodigo != t1.eprocodigo or (t0.eprocodigo is null and t1.eprocodigo is not null))
OR (t0.epropd  != t1.epropd  or (t0.epropd  is null and t1.epropd  is not null))
OR (t0.epropda != t1.epropda or (t0.epropda is null and t1.epropda is not null))

OR (t0.odflfechentrest   != t1.odflfechentrest   or (t0.odflfechentrest   is null and t1.odflfechentrest is not null))
OR (t0.odflfechentrega   != t1.odflfechentrega   or (t0.odflfechentrega   is null and t1.odflfechentrega is not null))
OR (t0.tsercodigo        != t1.tsercodigo        or (t0.tsercodigo        is null and t1.tsercodigo is not null))
OR (t0.tentcodigo        != t1.tentcodigo        or (t0.tentcodigo        is null and t1.tentcodigo is not null))
OR (t0.tpagcodigo        != t1.tpagcodigo        or (t0.tpagcodigo        is null and t1.tpagcodigo is not null))
OR (t0.ciudcodigoorigen  != t1.ciudcodigoorigen  or (t0.ciudcodigoorigen  is null and t1.ciudcodigoorigen is not null))
OR (t0.ciudcodigodestino != t1.ciudcodigodestino or (t0.ciudcodigodestino is null and t1.ciudcodigodestino is not null))
OR (t0.agencodigoorigen  != t1.agencodigoorigen  or (t0.agencodigoorigen  is null and t1.agencodigoorigen is not null))
OR (t0.agencodigodestino != t1.agencodigodestino or (t0.agencodigodestino is null and t1.agencodigodestino is not null))
OR (t0.comucodigo        != t1.comucodigo        or (t0.comucodigo        is null and t1.comucodigo is not null))
OR (t0.entrut            != t1.entrut            or (t0.entrut            is null and t1.entrut is not null))
OR (t0.entdv             != t1.entdv             or (t0.entdv             is null and t1.entdv is not null))
OR (t0.entapellidop      != t1.entapellidop      or (t0.entapellidop      is null and t1.entapellidop is not null))
OR (t0.entapellidom      != t1.entapellidom      or (t0.entapellidom      is null and t1.entapellidom is not null))
OR (t0.entnombres        != t1.entnombres        or (t0.entnombres        is null and t1.entnombres is not null))
OR (t0.entfecha          != t1.entfecha          or (t0.entfecha          is null and t1.entfecha is not null))

INTO OUTFILE '/home/procesos_carga/act_estado_movil/mv_of_estado_update.txt' -- pathFileUpdate
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';


END $$

DELIMITER ;