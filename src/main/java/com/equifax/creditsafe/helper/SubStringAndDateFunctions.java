package com.equifax.creditsafe.helper;


import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class SubStringAndDateFunctions {

	
	private static String checkForNull(String input) {

		if (null != input && input.trim().length() > 0) {

			if ("NULL".equalsIgnoreCase(input.trim())) {
				return null;
			}

			return input;
		}

		return null;
	}

	public static String subStringResponse(String originalString, int startingPostion, int numberOfCharacters, boolean doTrim) {
		
		int actualStartingPoint=startingPostion-1;
		int substringTill = actualStartingPoint+numberOfCharacters;
		if(doTrim) {
		
		String OutputResponse= (originalString.substring(actualStartingPoint, substringTill)).trim();
		
		return OutputResponse;
	}
	else
		return (originalString.substring(actualStartingPoint,substringTill));
	}
	
	public static String subStringPhone(String originalString, int startingPostion, int numberOfCharacters, boolean doTrim) {
		
		int actualStartingPoint=startingPostion-1;
		int substringTill = actualStartingPoint+numberOfCharacters;
		if(doTrim) {
			//System.out.println("Substring Result: "+startingPostion+"--"+originalString.substring(startingPostion, startingPostion+numberOfCharacters));
			
		String outputResponse= (originalString.substring(actualStartingPoint, substringTill)).trim();
		
		if(outputResponse.equals("0000000000") || outputResponse.equals("")) {
			outputResponse=null;
		}
		
		return outputResponse;
	}
	else
		return (originalString.substring(actualStartingPoint,substringTill));
	}
	
	public static DateTime getCurrentDate(){		
		DateTime dt = new DateTime();
		return dt;
		
	}
	
	public static DateTime subStringDate(String originalString, int startingPostion, int numberOfCharacters, boolean doTrim, String format) {
		DateTime out = null;
		int actualStartingPoint=startingPostion-1;
		int substringTill = actualStartingPoint+numberOfCharacters;
		if(doTrim) {
			
				
			String outputResponse= (originalString.substring(actualStartingPoint, substringTill)).trim();
			
			if(outputResponse.equals("00000000") || outputResponse.equals("")) {
				out=null;
			}
			else
			{
				DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
				long millis = formatter.parseMillis(outputResponse);
				out = new DateTime(millis);	
			}
		}
		return out;		
	}

}
