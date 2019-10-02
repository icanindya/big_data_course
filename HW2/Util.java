import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class Util {
	public static int getAge(String dob){
		Date birthDate = new Date();
		Date currentDate = new Date();
		try {
			birthDate = new SimpleDateFormat("MM/dd/yyyy").parse(dob);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return getDiffYears(birthDate, currentDate);
	}
	
	public static int getDiffYears(Date first, Date last) {
	    Calendar a = getCalendar(first);
	    Calendar b = getCalendar(last);
	    int diff = b.get(Calendar.YEAR) - a.get(Calendar.YEAR);
	    if (a.get(Calendar.MONTH) > b.get(Calendar.MONTH) || 
	        (a.get(Calendar.MONTH) == b.get(Calendar.MONTH) && a.get(Calendar.DATE) > b.get(Calendar.DATE))) {
	        diff--;
	    }
	    return diff;
	}

	public static Calendar getCalendar(Date date) {
	    Calendar cal = Calendar.getInstance(Locale.US);
	    cal.setTime(date);
	    return cal;
	}
	
	public static void main(String[] args){
		System.out.println(getAge("1/1/1960"));
		System.out.println(getAge("1/1/1990"));
		System.out.println(getAge("1/1/1950"));
		System.out.println(getAge("1/1/1980"));
		System.out.println(getAge("1/1/1950"));
		System.out.println(getAge("1/1/1980"));
		
	}
}
