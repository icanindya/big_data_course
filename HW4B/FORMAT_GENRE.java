import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE extends EvalFunc<String>{ 

   public String exec(Tuple input) throws IOException {   
      String formatted = " ";
      String str = (String)input.get(0);
      
      String[] genres = str.split("\\|");
      if(genres.length == 1){
    	  formatted += "1) " + genres[0] + " ixa140430";
      }
      else if(genres.length == 2){
    	  formatted += "1) " + genres[0] + " & 2) " + genres[1] + " ixa140430";
      }
      else{
    	  for(int i = 0; i < genres.length - 2; i++){
        	  formatted += i+1 + ") " + genres[i] + ", ";
          }
    	  formatted += genres.length - 1 + ") " + genres[genres.length - 2];
    	  formatted += " & " + genres.length + ") " + genres[genres.length - 1] + " ixa140430";
      }
      return formatted;  
   } 
}
