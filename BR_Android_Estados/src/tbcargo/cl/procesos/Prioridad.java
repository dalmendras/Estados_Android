package 	tbcargo.cl.procesos;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Period;



public class Prioridad {
	
	
	Prioridad () {
		
	}
	
	
	public static String[] setDatequery() {
		
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
