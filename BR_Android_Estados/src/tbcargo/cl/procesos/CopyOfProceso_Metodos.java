package tbcargo.cl.procesos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import tbcargo.cl.conn.DB_Dls;
import tbcargo.cl.conn.DB_MySql;
import tbcargo.cl.conn.DB_MySql_Amazon;



/** Clase Abstracta de métodos carga y descarga 
 * @author David
 */
public class CopyOfProceso_Metodos {

	
	/**
	 * Chequea si archivo está en ejecución en linux
	 * @param filename Nombre del archivo (Ej: archivo.jar)
	 * @return True "Está en ejecución" False "No está en Ejecución"
	 */
	public boolean is_check_process_run(String filename) {
		
		try {
			//pstree -a -p
			String cmd1 = "pgrep -f " + filename;
			System.out.println("Ejecutando comando: " + cmd1);
			Process proc1 = Runtime.getRuntime().exec(cmd1);                        
			proc1.waitFor();
			
			BufferedReader buf = new BufferedReader(new InputStreamReader(proc1.getInputStream()));
			
			String line = "";
			String output = "";
			int i = 0;
			
			while ((line = buf.readLine()) != null) {
				System.out.println("Linea: " + line);
				output += line + "\n";
				i++;
			}
			
			if ( i <= 2 ) {
				System.out.println("output: " + output);
				System.out.println("Proceso no está en curso. Se puede ejecutar.");
				return true;
			} else {
				System.out.println("output: " + output);
				System.out.println("Proceso esta en cuerso. No se puede ejecutar.");
				return false;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	
	/**Método borra archivos en linux local
	 * 
	 * @param rutaarchivo Indicar ruta completa incluyecto archivo y extensión
	 * @return True "Borrado exitoso archivos" False "Borrado fallido archivos"
	 */
	public boolean setBorrarFileLocal(String rutaarchivo[]) {		
		
		try {
			
			for (String pathinup : rutaarchivo) {
			
				File file = new File(pathinup);
				
					if (file.delete()) {
						//System.out.println("Archivo: " + file.getName() + " borrado!");
					} else {
						file.delete();
						//System.out.println("Archivo: " + file.getName() + " no encontrado");
					}
				}
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}		
		
	}
	
	
	/**Método vuelca query Dls a txt
	 * 
	 * @param query Sentencia query
	 * @param pathfile Ruta destino txt
	 * @return True "Volcado exitoso en ruta" False "Volcado fallido"
	 */
	public boolean setDlsToTxt(String query, String pathfile) {
		
		try {
			Connection con_DlsProd = DB_Dls.getInstance().getConnection();
			Statement stmt_mySql = con_DlsProd.createStatement();

			ResultSet rs_mySql = stmt_mySql.executeQuery(query);
			ResultSetMetaData rsmd = rs_mySql.getMetaData();
			
			int cols = rsmd.getColumnCount();
			
			//String filename = "F:\\dw_mv_of_estado_temp.txt"; "/home/procesos_carga/act_estados_web/mv_of_ctacte_temp.txt";
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pathfile), "UTF8"));
		 	String cleanTab = null;
		 	String cleanTab2 = null;
		 	Integer tipfield = null;
			
		 	while (rs_mySql.next()) {
		 		
		 		for (int x = 1; x <= cols; x++ ) {
					   
					String field = rs_mySql.getString(x);
					if (!rs_mySql.wasNull()) {
		 				
		 				tipfield = rsmd.getColumnType(x);
		 				
		 				if (tipfield == 1 || tipfield == -15 || tipfield == -9 || tipfield == 12) {
		 					
		 					cleanTab = rs_mySql.getString(x).trim();
		 					cleanTab2 = cleanTab.replaceAll("\t", "");
		 					cleanTab2 = cleanTab2.replaceAll("\n", "");
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
	 					} else if (x == cols) {
							out.write("\\N");
						}
 					}
	 			}
	 			out.write('\n');
 			}
		 	
		 	out.flush();
		 	out.close();
		 	
		 	rs_mySql.close();
		 	stmt_mySql.close();
		 	con_DlsProd.close();
		 	
		 	return true;
		 				
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}		
	
	}
	
	
	/**Método lee txt e inserta en tabla traspaso a bd local previo truncamiento
	 * 
	 * @param tablaName Nombre de la tabla donde insertar datos de txt
	 * @param pathfile Ruta y Archivo donde se encuentra txt
	 * @return True "Inserción exitosa de txt a bd local" False "Falla en inserción txt a bd local"
	 */
	public boolean setTxtToMysqlLocalTemp(String schema, String tablename, String pathfile) {

		try {
			//String tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Upload a Mysql_Local: " + tmo);
			Connection con_mySql = DB_MySql.getInstance().getConnection();
			Statement stmt_mySql = con_mySql.createStatement();
			
			// NO COMMIT
			con_mySql.setAutoCommit(false);
		
			String ssql = "TRUNCATE TABLE " + schema + "." + tablename;        //String ssql = "TRUNCATE TABLE mv_of_ctacte_temp";
			stmt_mySql.executeUpdate(ssql);
			
			stmt_mySql.executeUpdate( getStmpLoadDataInfile(schema, tablename, pathfile) );
			
			// COMMIT
			con_mySql.commit();			
			
			stmt_mySql.close();
			con_mySql.close();
			
			//tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Fin Upload a Mysql_Local: " + tmo);
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}

	
	/**Método llama sp y genera txt para insert y update (Caso OF)
	 * 
	 * @return True "Ejecución exitosa de almacenado y txt insert/updates OF ctacte Web" False "Falla de Almacenado txt insert/updates OF ctacte Web"
	 */
	public boolean setMysqlLocalTxtInsertUpdate(String sp_stmp_name) {
		
		try (	Connection con_mySql = DB_MySql.getInstance().getConnection();
				Statement stmt_mySql = con_mySql.createStatement();
				) {
			//String tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Generacion TXT Inserts/Updates: " + tmo);
			
			con_mySql.setAutoCommit(false);
					
			String ssql = sp_stmp_name;
			stmt_mySql.executeUpdate(ssql);
			
			con_mySql.commit();
			
			//tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Fin Generacion TXT Inserts/Updates: " + tmo);
			return true;
						
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}

	
	/**Método inserta txt a tabla mysql local
	 * 
	 * @param tablename Nombre de la tabla donde insertar datos de txt
	 * @param pathfile Ruta y Archivo donde se encuentra txt
	 * @return True "Inserción exitosa de txta bd local" False "Falla en inserción de txt a bd local"
	 */
	public boolean setInsertsToMysqlLocal(String schema, String tablename, String pathfile){
		
		try (
				Connection con_mySql = DB_MySql.getInstance().getConnection();
				Statement stmt_mySql = con_mySql.createStatement();
				){
			//String tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Inicio Inserts Mysql Local: " + tmo);
			
			// NO COMMIT
			con_mySql.setAutoCommit(false);
			
			//System.out.println(ssql);
			stmt_mySql.executeUpdate(getStmpLoadDataInfile(schema, tablename, pathfile));

			con_mySql.commit();
			//con_mySql.close();
			
			//tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Fin Inserts Mysql Local: " + tmo);
			return true;
						
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
		
	
	/**Método inserta txt a tabla mysql amazon
	 *  
	 * @param tablename Nombre de la tabla donde insertar datos de txt
	 * @param pathfile Ruta y Archivo donde se encuentra txt
	 * @return True "Inserción exitosa de txt a bd amazon" False "Falla en inserción de txt a bd amazon"
	 */
	public boolean setInsertsToAmazon(String tunnelAmazon, String schema, String tablename, String pathfile){
		
		//System.out.println("Proceso setInsertToAmazon");
		//new EnvioNotaMail("setInsertsToAmazon ", "Proceso setInsertToAmazon");
		
		try {
			
			//String tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Inicio Inserts Mysql Amz: " + tmo);
			//System.out.println("Abriendo Tunel SSH");
			
			Process proc1 = Runtime.getRuntime().exec(tunnelAmazon);                        
			
			int processComplete = proc1.waitFor();
			
			//System.out.println("proc1: " + processComplete);
			//new EnvioNotaMail("setInsertsToAmazon ", "processComplete(valor): " + processComplete);
			
			if (processComplete == 0) {
				
				//System.out.println("Actualizacion Satisfactoria.");
				//System.out.println(pathfile);
				//System.out.println("Tunel Abierto");
				//System.out.println("Insertando ...");
				
				Connection con_mySql = DB_MySql_Amazon.getInstance().getConnection();
				Statement stmt_mySql = con_mySql.createStatement();
				
				// NO COMMIT
				con_mySql.setAutoCommit(false);
				
				stmt_mySql.executeUpdate( getStmpLoadDataInfile(schema, tablename, pathfile) );
				
				con_mySql.commit();
				
				stmt_mySql.close();
				con_mySql.close();
				
				//tmo = Calendar.getInstance().getTime().toString();
				//System.out.println("Fin Inicio Inserts Mysql Amz: " + tmo);
				return true;
				
			} else {
				System.out.println("Error en la actualización.");
				//new EnvioNotaMail("setInsertsToAmazon ", "Error en la actualización: ");
				return false;
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	
	/**Método ejecuta script sql en BD local
	 * 
	 * @param pathfile Ruta y Archivo donde se encuentra txt
	 * @param dbName Nombre de la BD
	 * @return True "Ejecución exitosa de script sql en bd local" False "Falla de ejecución de script sql en bd local"
	 */
	public boolean setUpdatesToMysqlLocal(String[] executeCmd){
		
		try {
			
			//String tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Inicio Updates Mysql_Local: " + tmo);
			
			//String dbUserName = "root";
			//String dbPassword = "lamisma1";
			//String source = "/home/procesos_carga/act_estados_web/updateofweb.txt";			        
		   	//String dbName = "descarga_test";
			
		   	//String[] executeCmd = new String[]{"mysql", "--user=" + dbUserName, "--password=" + dbPassword, dbName,"-e", "source " + pathfile};
			 
		   	Process runtimeProcess;
		    
            runtimeProcess = Runtime.getRuntime().exec(executeCmd);
            int processComplete = runtimeProcess.waitFor();
 
            if (processComplete == 0) {
                //System.out.println("Actualizacion Satisfactoria.");
               
            	return true;
            } else {
                System.out.println("Error en la actualización.");
                return false;
            }
		       		 
			//tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Fin Updates Mysql_Local: " + tmo);
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
		
	
	/**Método ejecuta script sql en amazon
	 * 
	 * @param dbname Nombre de la BD
	 * @param path Ruta
	 * @param file Nombre archivo
	 * @return True "Ejecución exitosa de script sql en bd local" False "Falla de ejecución de script sql en bd local" 
	 */
	public boolean setUpdatesToAmazon(String schema, String path, String file){
		
		try{
			
			//String tmo = Calendar.getInstance().getTime().toString();
			//System.out.println("Inicio Update to Mysql Cloud: " + tmo);
				
			//System.out.println("Transferecia TXT Update ... ");
			
			//String cmd3="scp -P 443 -i /opt/Amazon/pem/loginPrueba.pem ";
			//cmd3 += path + file + " ";                               //"/home/procesos_carga/act_estados_web/updateofweb.txt ";
			//cmd3 += " root@54.186.202.242:" + path;                  //" root@54.186.202.242:/home/procesos_carga/act_estados_web/";
			//cmd3 += " root@" + ipAmazon + ":" + path;                  //" root@54.186.202.242:/home/procesos_carga/act_estados_web/";
			
			//System.out.println("Comando Ejecución: " + cmd3);
			
			String cmd3 = DB_MySql_Amazon.getStmOpenFileTunnel(path, file);

			Process proc3 = Runtime.getRuntime().exec(cmd3);                        
			//proc3.waitFor();
			
            int processComplete = proc3.waitFor();
            
            if (processComplete == 0) {
                //System.out.println("Actualizacion Satisfactoria.");
               
            	//System.out.println("Transferecia Finalizada");
            	
            	//System.out.println("Actualizando ...");
            	//String cmd4 = "ssh -i /opt/Amazon/pem/loginPrueba.pem ";
            	//cmd4 += "root@54.186.202.242 -p 443 ";
            	//cmd4 += "root@" + ipAmazon + " -p 443 ";
            	
            	//cmd4 += "mysql -u root -pamazontbc " + dbname + " < ";   //cmd4 += "mysql -u root -pamazontbc descarga_test < ";
            	//cmd4 += path + file;                                     //cmd4 += "/home/procesos_carga/act_estados_web/updateofweb.txt";
            	
            	//System.out.println("Comando Ejecución: " + cmd4);
            	
            	String cmd4 = DB_MySql_Amazon.getStmOpenFileTunnelScriptLoad(schema, path, file);
            	
            	Process proc4 = Runtime.getRuntime().exec(cmd4);
            	//proc4.waitFor();
            	processComplete = proc4.waitFor();
            	
            	if (processComplete == 0) {
					return true;
				} else {
					return false;
				}
            	
            	//tmo = Calendar.getInstance().getTime().toString();
            	//System.out.println("Fin Upload Update Dif Base Local Mysql: " + tmo);
            	
            } else {
                System.out.println("Error en la actualización.");
                return false;
            }
			
			
		
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
			
	}
	
	
	private String getStmpLoadDataInfile(String schema, String tablename, String pathfile) {
		
		String ssql = "";
		
		ssql = "LOAD DATA LOCAL INFILE '" + pathfile + "'"; //ssql = "LOAD DATA LOCAL INFILE '/home/procesos_carga/act_estados_web/mv_of_ctacte_temp.txt'";
		ssql += " INTO TABLE " + schema + "." + tablename;
		ssql += " FIELDS TERMINATED BY '\t'";
		ssql += " LINES TERMINATED BY '\n'";
		
		return ssql;
		
	}
	
    
}
