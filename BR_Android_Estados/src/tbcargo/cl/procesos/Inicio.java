package tbcargo.cl.procesos;

import java.io.File;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.Calendar;



public class Inicio {

	/*private static final String ruta[] = {"/home/procesos_carga/act_estados_web/insertofweb.txt", 
										"/home/procesos_carga/act_estados_web/updateofweb.txt"};*/
	//private static String filejarname = "Estados_Of_Movil.jar";
	private static String filejarname = "";

	public static void main(String[] args) {
		
		Proceso_Metodos proceso = new Proceso_Metodos();
		
		if (proceso.is_check_process_run(getFilejarname())) {
			
			System.out.println("-- PROCESO OF MOVIL --");
			Proceso_Of_Est_Movil ofctacte = new Proceso_Of_Est_Movil();
			
			String tmo = Calendar.getInstance().getTime().toString();
			System.out.println("Fin Proceso Completo: " + tmo);
		}
		
		
	}
	
	private static String getFilejarname() {
		
		try {
			CodeSource codeSource = Inicio.class.getProtectionDomain().getCodeSource();
			File jarFile = new File(codeSource.getLocation().toURI().getPath());
			System.out.println(jarFile.getName());
			Inicio.filejarname = jarFile.getName();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return filejarname;
	}

}
