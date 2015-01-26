package 	tbcargo.cl.procesos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import tbcargo.cl.conn.DB_MySql;
import tbcargo.cl.conn.DB_MySql_Amazon;
import tbcargo.cl.conn.DB_Type;
import tbcargo.cl.notificacion.EnvioNotaMail;




public class Proceso_Of_Est_Movil {
	
	private final static String mySqlLocalSchema = DB_MySql.getMysqlschema();
	private final static String mySqlAmzSchema = DB_MySql_Amazon.getDbname();
	private final static String tunnelstmp = DB_MySql_Amazon.getTunnelamz();
	private final static String insertfile = "mv_of_estado_insert.txt";
	private final static String updatefile = "mv_of_estado_update.txt";
	private final static String table = "mv_of_estado";
	private final static String tabletmp = "mv_of_estado_temp";
	private final static String filetabletmp = "mv_of_estado_temp.txt";
	private final static String path = DB_MySql.getPathEtl();
	private final static String sp_stmp_name = "CALL sp_estado_of_movil()";
	private final static String pathinsertfile = path + insertfile;
	private final static String pathupdatefile = path + updatefile;
	private final static String pathfiletabletmp = path + filetabletmp;
	private static java.sql.Timestamp tmstmp_ini = new java.sql.Timestamp(new java.util.Date().getTime());
	private static java.sql.Timestamp tmstmp_fin = new java.sql.Timestamp(new java.util.Date().getTime());
	private final static String[] stmpExcUpSxLx = DB_MySql.getExecuteUpdateScriptLinux(pathupdatefile);
	private static String dateqry[];
	
	private final static String[] proceso_of_list = {"01_Borrado de archivos_Android         ",
													 "02_Volcado Dls a txt_Android           ",
													 "03_Insert txt Dls a Mysql Local_Android",
													 "04_Sp genera Insert/Update_Android     ",
													 "05_Insert OF en Amazon_Android         ",
													 "06_Insert OF en MySql Local_Android    ",
													 "07_Script Update en Amazon_Android     ",
													 "08_Script Update en Local_Android      ",
													 "09_Insert log MySql Amazon_Android     ",
													 "10_Insert log MySql Local_Android      "};
	
	Proceso_Of_Est_Movil () {
		
		
		if (setDatequery() != null) {
			
			setDateqry(setDatequery());
			
			Proceso_Metodos of_movil = new Proceso_Metodos();
			
			String className = this.getClass().getSimpleName();
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			setTmstmp_ini(new java.sql.Timestamp(new java.util.Date().getTime()));
			System.out.print(proceso_of_list[0] + " | " + dateFormat.format(getTmstmp_ini()));
			
			
			if (of_movil.setBorrarFileLocal(getPathFiles())) {
				System.out.println(" - " + dateFormat.format(new Date()) + " | ");
				System.out.print(proceso_of_list[1] + " | " + dateFormat.format(new Date()));
				
				if (of_movil.setBdToTxt(DB_Type.DLS, getQueryDlsOfCtacte(), pathfiletabletmp, false)) {
				//if (of_movil.setDlsToTxt(getQueryDlsOfCtacte(), pathfiletabletmp)) {
					System.out.println(" - " + dateFormat.format(new Date()) + " | ");
					System.out.print(proceso_of_list[2] + " | "  + dateFormat.format(new Date()));
					
					if (of_movil.setTxtToMysqlLocalTemp(mySqlLocalSchema, tabletmp, pathfiletabletmp, true)) {
					//if (of_movil.setTxtToMysqlLocalTemp(mySqlLocalSchema, tabletmp, pathfiletabletmp)) {
						System.out.println(" - " + dateFormat.format(new Date()) + " | ");
						System.out.print(proceso_of_list[3] + " | " + dateFormat.format(new Date()));
						
						if (of_movil.setMysqlLocalTxtInsertUpdate(sp_stmp_name)) {
							System.out.println(" - " + dateFormat.format(new Date()) + " | ");
							System.out.print(proceso_of_list[4] + " | " + dateFormat.format(new Date()));
							
							//if (of_movil.setInsertsToAmazon(DB_MySql_Amazon.getTunnelamz(), mySqlAmzSchema, table, pathinsertfile)) {
							if (of_movil.setInsertsToAmazon(DB_MySql_Amazon.getTunnelamz(), mySqlAmzSchema, table, pathinsertfile, true)) {
							//if (of_movil.setInsertsToAmazon(DB_MySql_Amazon.getTunnelamz(), mySqlAmzSchema, table, pathinsertfile)) {
								System.out.println(" - " + dateFormat.format(new Date()) + " | ");
								System.out.print(proceso_of_list[5] + " | " + dateFormat.format(new Date()));
								
								if (of_movil.setInsertsToMysqlLocal(mySqlLocalSchema, table, pathinsertfile, true)) {
								//if (of_movil.setInsertsToMysqlLocal(mySqlLocalSchema, table, pathinsertfile)) {
									System.out.println(" - " + dateFormat.format(new Date()) + " | ");
									System.out.print(proceso_of_list[6] + " | " + dateFormat.format(new Date()));
									
									if (of_movil.setUpdatesToAmazon(mySqlAmzSchema, path, updatefile)) {
										System.out.println(" - " + dateFormat.format(new Date()) + " | ");
										System.out.print(proceso_of_list[7] + " | " + dateFormat.format(new Date()));
										setTmstmp_fin(new java.sql.Timestamp(new java.util.Date().getTime()));
										
										if (of_movil.setUpdatesToMysqlLocal( stmpExcUpSxLx )) {
											//setTmstmp_fin(new java.sql.Timestamp(new java.util.Date().getTime()));
											System.out.println(" - " + dateFormat.format(getTmstmp_fin()) + " | ");
											System.out.print(proceso_of_list[8] + " | " + dateFormat.format(new Date()));
											
											if (insertMysqlLocal_log(DB_Type.MYSQL_AMZ)) {
												System.out.println(" - " + dateFormat.format(getTmstmp_fin()) + " | ");
												System.out.print(proceso_of_list[9] + " | " + dateFormat.format(new Date()));
												
												if (insertMysqlLocal_log(DB_Type.MYSQL_LOCAL)) {
													System.out.println(" - " + dateFormat.format(getTmstmp_fin()) + " | ");
													
												} else {
													new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[9]);
												}
											} else {
												new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[8]);
											}
										} else {
											new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[7]);
										}
									} else {
										new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[6]);
									}
								} else {
									new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[5]);
								}
							} else {
								new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[4]);
							}
						} else {
							new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[3]);
						}
					} else {
						new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[2]);
					}
				} else {
					new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[1]);
				}
			} else {
				new EnvioNotaMail("File: " + of_movil.getJarName() + " | Error Clase: " + className, "Proceso: " + proceso_of_list[0]);
			}
		} else {
			System.out.println("Inconsistencia de horarios descarga (5 min)");
		}
		
	}


	private String[] getPathFiles() {
		
		String ruta[] = {path + insertfile, path + updatefile};
		return ruta;
	}
	
	public static java.sql.Timestamp getTmstmp_ini() {
		return tmstmp_ini;
	}

	private static void setTmstmp_ini(java.sql.Timestamp tmstmp_ini) {
		Proceso_Of_Est_Movil.tmstmp_ini = tmstmp_ini;
	}

	public static java.sql.Timestamp getTmstmp_fin() {
		return tmstmp_fin;
	}

	private static void setTmstmp_fin(java.sql.Timestamp tmstmp_fin) {
		Proceso_Of_Est_Movil.tmstmp_fin = tmstmp_fin;
	}
		
	public static String[] getDateqry() {
		return dateqry;
	}	
	public static void setDateqry(String dateqry[]) {
		Proceso_Of_Est_Movil.dateqry = dateqry;
	}
	
	private String getQueryDlsOfCtacte() {
		
		
		String[] dates = getDateqry();
		String f_inicio = dates[0];
		String f_fin = dates[1];
		
		System.out.println("f_inicio: " + f_inicio);
		System.out.println("f_fin: " + f_fin);
		
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
		DateTime fec_ini = formatter.parseDateTime(f_inicio);
		DateTime fec_fin = formatter.parseDateTime(f_fin);
		
		String queryDls = null; 
		/*queryDls = "SELECT " +
					 "t0.odflcodigo, " +
					 "t0.eprocodigo " +
					 "FROM mv_orden_flete t0 " +
					 "WHERE t0.odflfechemicorta >= ADD_MONTHS(TODAY, -2) ";*/
		
		queryDls = "SELECT " + 
					"t0.odflcodigo, " +
					"to_char(t0.odflfechemicorta,'%Y-%m-%d') AS odflfechemicorta, " +
					"t0.eprocodigo, " +
					"CASE WHEN pd.odflcodigo >= 0 THEN 1 ELSE 0 END as epropd, " +
					"CASE WHEN t1.odflcodigo >= 0 THEN 1 ELSE 0 END as epropda " +
					"FROM mv_orden_flete t0 " +
					" LEFT JOIN (SELECT t0.odflcodigo " + 
					"		 FROM mv_orden_flete t0 " +
					"		 WHERE t0.eprocodigo IN (3, 4, 5, 19, 20) " + 
					"		 AND EXISTS (SELECT 1 " +
					"					FROM mv_encargo t5 " +  
					"					INNER JOIN ma_ubicacion_fisic t6 ON t6.ubifcodigo = t5.ubiccodigoactual " + 
					"					WHERE t5.odflcodigo = t0.odflcodigo " +
					"					AND t6.agencodigo != t0.agencodigoorigen)) AS pd ON pd.odflcodigo = t0.odflcodigo " + 
					" LEFT JOIN mv_entrega_pda    t1 ON t1.odflcodigo = t0.odflcodigo " +
					"WHERE t0.odflfechemicorta >= MDY(" + fec_ini.getMonthOfYear() + "," +  fec_ini.getDayOfMonth() + "," + fec_ini.getYear() + ") " +  //'" + f_inicio + "' " +
					"AND t0.odflfechemicorta < MDY(" + fec_fin.getMonthOfYear() + "," + fec_fin.getDayOfMonth() + "," + fec_fin.getYear() + ") "; //'" + f_fin + "' "; 
		
		
		return queryDls;
		
	}
	
	
	private boolean insertMysqlLocal_log(DB_Type db_type) {
		
		String sql = "INSERT into mv_proceso_log (proceslog_ts_ini, proceslog_ts_fin, idtipoproceso) VALUES (?,?,?)";
			
		try {
			
			if (db_type == DB_Type.MYSQL_LOCAL) {
				Connection con_mySql = DB_MySql.getInstance().getConnection();
				PreparedStatement stmt_mySql = con_mySql.prepareStatement(sql);
				
				stmt_mySql.setTimestamp(1, getTmstmp_ini());
				stmt_mySql.setTimestamp(2, getTmstmp_fin());
				stmt_mySql.setInt(3, 1);
				stmt_mySql.executeUpdate();
				
				stmt_mySql.close();
				con_mySql.close();
				
			} else if (db_type == DB_Type.MYSQL_AMZ) {
				String cmd1 = tunnelstmp;
				Process proc1 = Runtime.getRuntime().exec(cmd1);                        
				proc1.waitFor();
				
				Connection con_mySql = DB_MySql_Amazon.getInstance().getConnection();
				PreparedStatement stmt_mySql = con_mySql.prepareStatement(sql);

				stmt_mySql.setTimestamp(1, getTmstmp_ini());
				stmt_mySql.setTimestamp(2, getTmstmp_fin());
				stmt_mySql.setInt(3, 1);
				stmt_mySql.executeUpdate();
				
				stmt_mySql.close();
				con_mySql.close();
				
			} else {
				System.err.println("BD incorrecta");
				return false;
			}
			
		} catch (Exception e) {
			System.err.println(e);
			return false;
		} 
		
		return true;
	}

	
	private static String[] setDatequery() {
		
		String dateqry[] = new String[2];
		
		SimpleDateFormat simpledateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		//Parametros de fechas
		DateTime curdatetime = new DateTime();
		int curminround = (int) redon_mult_prox( curdatetime.getMinuteOfHour(), 5);
		
		DateTime startdate = new DateTime().withDayOfMonth(1).minus(Period.months(2));
	    DateTime starthighblock = new DateTime().minus(Period.days(25));
	    DateTime startmidblock = startdate;
		
	    Days dayse = Days.daysBetween(startdate, starthighblock);
	    
	    int daysdif = dayse.getDays() + 1;
	    int periodinter = 2;
	    
	    int inipd = (daysdif / periodinter) + daysdif % periodinter;
	    int midpd = (daysdif - inipd) / (periodinter - 1);
	    
	    startmidblock = startmidblock.plusDays(midpd);
		
		// Parametros de minutos
		Set<Integer> ciclohigh = new HashSet<Integer>(Arrays.asList(15, 25, 45, 55));
		int ciclomidone = 5;
		int ciclomidtwo = 35;
		
		System.out.println("curminround: " + curminround);
		
		if ( curminround == ciclomidone ) {
			
			System.out.println("Ciclo 1: min 5 ");
			
			dateqry[0] = simpledateFormat.format(startmidblock.toDate());
			dateqry[1] = simpledateFormat.format(starthighblock.toDate());
			
			/*System.out.println("Fecha 1: ini >= " + simpledateFormat.format(startmidblock.toDate()));
			System.out.println("Fecha 1: fin < " + simpledateFormat.format(starthighblock.toDate()));*/
			
			return dateqry;
			
		} else if ( curminround == ciclomidtwo ) {
			
			System.out.println("Ciclo 2: min 35 ");
			
			dateqry[0] = simpledateFormat.format(startdate.toDate());
			dateqry[1] = simpledateFormat.format(startmidblock.toDate());
			
			/*System.out.println("Fecha 2: ini >= " + simpledateFormat.format(startdate.toDate()));
			System.out.println("Fecha 2: fin < " + simpledateFormat.format(startmidblock.toDate()));*/
			
			return dateqry;
			
		} else if ( ciclohigh.contains(curminround) ) {
			
			System.out.println("Ciclos 3: min 15, 25, 45, 55 ");
			
			curdatetime = curdatetime.plus(Period.days(1));
			
			dateqry[0] = simpledateFormat.format(starthighblock.toDate());
			dateqry[1] = simpledateFormat.format(curdatetime.toDate());
			
			/*System.out.println("Fecha 3: ini >= " + simpledateFormat.format(starthighblock.toDate()));
			System.out.println("Fecha 3: ini < " + simpledateFormat.format(curdatetime.toDate()));*/
			
			return dateqry;
			
		}
		
		return null;
		
	}
	
	
	private static int redon_mult_prox( double num, int multipleOf ) {
		
	    if (num % multipleOf == 0)
	        return (int) num;
	    else if (num % multipleOf < multipleOf/2)
	    	return (int) (num - num % multipleOf);
	    else
	    	return (int) (num + (multipleOf - num % multipleOf));
	    
	}	
	
	
}
