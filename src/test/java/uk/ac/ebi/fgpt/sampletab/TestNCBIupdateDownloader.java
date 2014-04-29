package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Calendar;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class TestNCBIupdateDownloader extends TestCase{

	
	/**	public void testDate(){
			assertEquals("checking current year", 2014, Calendar.getInstance().get(Calendar.YEAR) );
			assertEquals("checking current month", 04, Calendar.getInstance().get(Calendar.MONTH)+1);
			assertEquals("checking current date", 24, Calendar.getInstance().get(Calendar.DATE));
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -7);
			assertEquals("checking date 7 days ago", 17, cal.get(Calendar.DATE));
			System.out.println("DATE 7 DAYS AGO IS: " + cal.get(Calendar.DATE));
		}*/
		
		
		
		public void testSearchURL() throws Exception {
			HashSet<String> uids = new HashSet<String>();
			Calendar c = Calendar.getInstance();
			int currentyear = c.get(Calendar.YEAR);
			int currentmonth = c.get(Calendar.MONTH)+1; //zero based
			int currentdate = c.get(Calendar.DATE);
			c.add(Calendar.DATE, -7);
			int rangedate = c.get(Calendar.DATE);
			int rangemonth;
			int rangeyear;
			if(currentmonth==1){
				rangemonth =12;
				rangeyear = currentyear-1;
			}else{
				rangemonth = currentmonth -1 ;
				rangeyear = currentyear;
			}
			String startyear = Integer.toString(rangeyear);
			String startmonth = Integer.toString(rangemonth);
			String endyear = Integer.toString(currentyear);
			String endmonth = Integer.toString(currentmonth);
			String dateRange = startyear+"/"+startmonth+"/"+currentdate+":"+endyear+"/"+endmonth+"/"+rangedate;
			String term = dateRange+"[MDAT]+OR+"+dateRange+"[PDAT]+AND+public[Filter]";
			String strUrl = ("http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=biosample&term="+term+"&usehistory=y&retmax=100000");

		    try {
		    	//System.out.println("string url is :" + strUrl);
		        URL url = new URL(strUrl);
		        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
		       // urlConn.setRequestMethod("GET");
		       urlConn.connect();

		     assertEquals(HttpURLConnection.HTTP_OK, urlConn.getResponseCode());
		        
		    	BufferedReader in = new BufferedReader(
				        new InputStreamReader(urlConn.getInputStream()));
				String inputLine;
		//		StringBuffer response = new StringBuffer();
				String count = null;
				
				while ((inputLine = in.readLine()) != null) {
					Pattern countPattern = Pattern.compile("<Count>(.*?)</Count>");
					Matcher matcher1 = countPattern.matcher(inputLine);
					Pattern queryKeyPat = Pattern.compile("<QueryKey>(.*?)</QueryKey>");
					Matcher matcher2 = queryKeyPat.matcher(inputLine);
					Pattern webEnvPat = Pattern.compile("<WebEnv>(.*?)</WebEnv>");
					Matcher matcher3 = webEnvPat.matcher(inputLine);
					Pattern idPat = Pattern.compile("<Id>(.*?)</Id>");
					Matcher matcher4 = idPat.matcher(inputLine);
					
					if (matcher1.find()&& count==null){
						count = matcher1.group(1);
						//System.out.println("Count is :" + matcher1.group(1));
					}
					if (matcher2.find()){
						//System.out.println("QueryKey is: " + matcher2.group(1));
					}
					if (matcher3.find()){
						//System.out.println("WebEnv is : " + matcher3.group(1));
					}
				 if (matcher4.find()){
					 	uids.add(matcher4.group(1));
					}
					
				}
				in.close();
		    } catch (IOException e) {
		        System.err.println("Error creating HTTP connection");
		        e.printStackTrace();
		        throw e;
		    }
		    
		    for(String uid: uids){
		    	String fetch_url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=biosample&id="+uid+"&retmode=xml&rettype=full";
		    	URL url = new URL (fetch_url);
		    	HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
		    	urlConn.connect();

			    assertEquals(HttpURLConnection.HTTP_OK, urlConn.getResponseCode());
			    
			    break;
		    	
		    	//comment out all the following lines if you dont want it to download the biosample files
		    	/**URL fetch = new URL(fetch_url);
		    	ReadableByteChannel rbc = Channels.newChannel(fetch.openStream());
		    	FileOutputStream fos = new FileOutputStream(uid+"_NCBI.xml");
		    	fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);*/
		    
		    
		    
		}
		
		
		
		}
		
		
}
