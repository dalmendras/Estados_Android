DELIMITER $$

DROP FUNCTION IF EXISTS `GetDistance` $$
CREATE DEFINER=`root`@`%` FUNCTION `GetDistance`(

 lat1  decimal (10,7),
 lon1  decimal (10,7),
 lat2  decimal (10,7),
 lon2  decimal (10,7)

 ) RETURNS decimal(10,1)
    READS SQL DATA
BEGIN

  DECLARE  x  decimal (20,10);
  DECLARE  pi  decimal (21,20);
  SET  pi = 3.14159265358979323846;
  SET  x = sin( lat1 * pi/180 ) * sin( lat2 * pi/180  ) + cos(
 lat1 *pi/180 ) * cos( lat2 * pi/180 ) * cos(  abs( (lon2 * pi/180) -
 (lon1 *pi/180) ) );
  SET  x = acos( x );
  RETURN  ( 1.852 * 60.0 * ((x/pi)*180) );

END $$

DELIMITER ;