package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class WebUtils {

	public static String downloadURL(String url) throws IOException{
		StringBuffer buffer = new StringBuffer();
		BufferedReader input = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
		String line;
		while ((line = input.readLine()) != null){
			buffer.append(line);
		}
		return buffer.toString();
	}
}
