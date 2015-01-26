package tbcargo.cl.procesos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Calendar;

import tbcargo.cl.conn.DB_Dls;
import tbcargo.cl.conn.DB_MySql;
import tbcargo.cl.conn.DB_MySql_Amazon;

public class Procesos {

	
	public boolean borrado_inicial(String ruta[]) {
		
		try {
			
			for (String pathinup : ruta) {
			
				File file = new File(pathinup);
				
					if (file.delete()) {
						System.out.println("Archivo: " + file.getName() + " borrado!");
					 } else {
						file.delete();
						System.out.println("Archivo: " + file.getName() + " no encontrado");
					}
			   }
			return true;
			
		} catch (Exception e) {
				e.printStackTrace();
				return false;
		}
	}
	
	
	public boolean dlsToTxt() {
		
		try {
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nDescarga DLS a TXT ...: " + tmo);
			Connection con_DlsProd = DB_Dls.getInstance().getConnection();
			Statement stmt_mySql = con_DlsProd.createStatement();

			ResultSet rs_mySql = stmt_mySql.executeQuery("SELECT " +
														 "t0.odflcodigo, " +
														 "t0.eprocodigo " +
														 "FROM mv_orden_flete t0 " +
														 "WHERE t0.odflfechemicorta >= ADD_MONTHS(TODAY, -2) " +
														 "AND t0.odflfechemicorta <= TODAY");
			ResultSetMetaData rsmd = rs_mySql.getMetaData();
			
			int cols = rsmd.getColumnCount();
			
			//String filename = "F:\\dw_mv_of_estado_temp.txt";
			String filename = "/home/procesos_carga/act_estados/dw_mv_of_estado_temp.txt";
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "utf-8"));
		 	String cleanTab = null;
		 	String cleanTab2 = null;
		 	Integer tipfield = null;
			
		 	while (rs_mySql.next()) {
		 		
		 		for (int x = 1; x <= cols; x++ ) {
					   
		 			if (rs_mySql.getString(x) != null){
		 				
		 				tipfield = rsmd.getColumnType(x);
		 				
		 				if (tipfield == 1 || tipfield == -15 || tipfield == -9 || tipfield == 12) {
		 					
		 					cleanTab = rs_mySql.getString(x).trim();
		 					cleanTab2 = cleanTab.replaceAll("\t", " ");
		 					cleanTab2 = cleanTab2.replaceAll("\n", " ");
		 					cleanTab2 = cleanTab2.replaceAll("\"", "");
		 					out.write(cleanTab2);
		 					if (x < cols) {
		 						out.write('\t');		 						
		 					}
		 					
	 					} else {
	 						
	 						out.write(rs_mySql.getString(x).trim());
	 						if (x < cols) {
	 							out.write('\t');	 							
	 						}
 						}
		 				
	 				} else {
	 					if (x < cols) {
	 						out.write("\\N\t");	 						
	 					}
 					}
	 			}
	 			out.write('\n');
 			}
		 	
		 	out.flush();
		 	out.close();
		 	
		 	stmt_mySql.close();
		 	rs_mySql.close();
		 	con_DlsProd.close();
		 	
		 	tmo = Calendar.getInstance().getTime().toString();
		 	System.out.println("\nFin Descarga DLS a TXT: " + tmo);
		 	
		 	return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	
	}
		
		
	public boolean txtToMysqlLocal() {
		
		try {
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nUpload a Mysql_Local ...  " + tmo);
			Connection con_mySql = DB_MySql.getInstance().getConnection();
			Statement stmt_mySql = con_mySql.createStatement();
			
			// NO COMMIT
			con_mySql.setAutoCommit(false);
		
			String	ssql = "TRUNCATE TABLE dw_mv_of_estado_temp";
			stmt_mySql.executeUpdate(ssql);
			
			ssql = "LOAD DATA LOCAL INFILE '/home/procesos_carga/act_estados/dw_mv_of_estado_temp.txt'";
			
			ssql += " INTO TABLE dw_mv_of_estado_temp";
			ssql += " FIELDS TERMINATED BY '\t'";
			ssql += " LINES TERMINATED BY '\n'";
			//System.out.println(ssql);
			stmt_mySql.executeUpdate(ssql);
			
			// COMMIT
			con_mySql.commit();
			con_mySql.close();
			
			tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nFin Upload a Mysql_Local. " + tmo);
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
		
		
	public boolean MysqlLocalTxtInsertUpdate() {
		
		try {
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nGeneracion TXT Inserts/Updates ... " + tmo);
			Connection con_mySql = DB_MySql.getInstance().getConnection();
			Statement stmt_mySql = con_mySql.createStatement();
			
			// NO COMMIT
			con_mySql.setAutoCommit(false);
					
			String	ssql = "CALL sp_estado_of_movil()";
			stmt_mySql.executeUpdate(ssql);
			
			// COMMIT
			con_mySql.commit();
			
			con_mySql.close();
			
			tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nFin Generacion TXT Inserts/Updates. " + tmo);
			
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
		
		
	public boolean insertsToMysqlLocal() {
		
		try {
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nInicio Inserts Mysql Local ... " + tmo);
			Connection con_mySql = DB_MySql.getInstance().getConnection();

			Statement stmt_mySql = con_mySql.createStatement();
			
			// NO COMMIT
			con_mySql.setAutoCommit(false);
		
			String ssql;
			
			ssql = "LOAD DATA LOCAL INFILE '/home/procesos_carga/act_estados/insertof.txt'";
			
			ssql += " INTO TABLE dw_mv_of_estado";
			ssql += " FIELDS TERMINATED BY '\t'";
			ssql += " LINES TERMINATED BY '\n'";
			//System.out.println(ssql);
			stmt_mySql.executeUpdate(ssql);

			// COMMIT
			con_mySql.commit();
			
			stmt_mySql.close();
			con_mySql.close();
			
			tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nFin Inserts Mysql Local. " + tmo);
			
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
		
		
	public boolean insertsToCloud() {
		
		try {
			
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nInicio Inserts Mysql Cloud ... " + tmo);
			
			System.out.println("Abriendo Tunnel SSH ...");
			
			String cmd1 = "";
			
			cmd1 += "ssh -f -N -q -i /opt/Amazon/pem/loginPrueba.pem ";
			cmd1 += "-L 13306:localhost:3306 ubuntu@54.186.202.242";
			
			Process proc1 = Runtime.getRuntime().exec(cmd1);                        
			proc1.waitFor();
			
			System.out.println("\nTunnel Abierto.");
			
			System.out.println("\nInsertando valores nuevos ...");
						
			Connection con_mySql = DB_MySql_Amazon.getInstance().getConnection();
			Statement stmt_mySql = con_mySql.createStatement();
			
			// NO COMMIT
			con_mySql.setAutoCommit(false);
			
			String	ssql = "";
			
			ssql = "LOAD DATA LOCAL INFILE '/home/procesos_carga/act_estados/insertof.txt'";
			//ssql = ssql + " INTO TABLE " + tabla;
			ssql += " INTO TABLE dw_mv_of_estado";
			ssql += " FIELDS TERMINATED BY '\t'";
			ssql += " LINES TERMINATED BY '\n'";
			//System.out.println(ssql);
			stmt_mySql.executeUpdate(ssql);
			
			// COMMIT
			con_mySql.commit();			
			
			stmt_mySql.close();
			con_mySql.close();
			
			tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nFin Inicio Inserts Mysql Cloud. " + tmo);
			
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
		
		
	public boolean updatesToMysqlLocal() {
		
		try {
			
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nInicio Updates Mysql_Local ... " + tmo);
			
			String dbUserName = "root";
			String dbPassword = "lamisma1";
			String source = "/home/procesos_carga/act_estados/updateof.txt";			        
			String dbName = "dlsmaestros";
			
			String[] executeCmd = new String[]{"mysql", "--user=" + dbUserName, "--password=" + dbPassword, dbName,"-e", "source "+source};
			
			Process runtimeProcess;
			
			runtimeProcess = Runtime.getRuntime().exec(executeCmd);
			int processComplete = runtimeProcess.waitFor();
			
			if (processComplete == 0) {
				//System.out.println("Actualizacion Satisfactoria.");
			} else {
				System.out.println("Error en la actualización.");
				return false;
			}
			
			tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nFin Updates Mysql_Local ... " + tmo);
			
			return true;

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
		
		
	public boolean updatesToCloud() {
		
		try {
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nInicio Update to Mysql Cloud ... " + tmo);
			
			String cmd3 = "";
			System.out.println("Transferecia TXT Update a Server Remoto ... ");
			cmd3 += "scp -i /opt/Amazon/pem/loginPrueba.pem ";
			cmd3 += "/home/procesos_carga/act_estados/updateof.txt ";
			cmd3 += "root@54.186.202.242:/home/procesos_carga/act_estados/";
			
			Process proc3 = Runtime.getRuntime().exec(cmd3);                        
			proc3.waitFor();
			
			System.out.println("\nTransferecia Finalizada.");
			
			System.out.println("\nActualizando ...");
			
			String cmd4 = "ssh -i /opt/Amazon/pem/loginPrueba.pem ";
			cmd4 += "root@54.186.202.242 ";
			cmd4 += "mysql -u root -pamazontbc android < ";
			cmd4 += "/home/procesos_carga/act_estados/updateof.txt";
			
			Process proc4 = Runtime.getRuntime().exec(cmd4);                        
			proc4.waitFor();
			
			tmo = Calendar.getInstance().getTime().toString();
			System.out.println("\nFin Upload Update Dif Base Local Mysql ... " + tmo);
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
	
	
}