package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author drashtti This tool downloads all the sample information from NCBI
 *         biosamples based on the modification and publication date within the
 *         last week
 */
public class NCBIupdateDownloader {

	private int delay = 0;
	// private int maxdelay = 3;
	private String base = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/";
	private final String USER_AGENT = "Mozilla/5.0";
	private HashMap<String, String> results = new HashMap<String, String>();
	private HashSet<String> uids = new HashSet<String>();

	/**
	 * performs esearch to get all samples that have been updated or published
	 * for a range of dates and returns a map with all the results the keys are
	 * db, count, query_key, WebEnv and uids
	 */
	public void esearch() {

		Calendar c = Calendar.getInstance();
		int currentyear = c.get(Calendar.YEAR);
		int currentmonth = c.get(Calendar.MONTH) + 1; // zero based
		int currentdate = c.get(Calendar.DATE);
		c.add(Calendar.DATE, -7);
		int rangedate = c.get(Calendar.DATE);
		int rangemonth;
		int rangeyear;
		if (currentmonth == 1 && (currentdate <= 7)) {
			rangemonth = 12;
			rangeyear = currentyear - 1;
		} else {
			rangemonth = currentmonth - 1;
			rangeyear = currentyear;
		}
		String startyear = Integer.toString(rangeyear);
		String startmonth = Integer.toString(rangemonth);
		String endyear = Integer.toString(currentyear);
		String endmonth = Integer.toString(currentmonth);
		String dateRange = startyear + "/" + startmonth + "/" + currentdate
				+ ":" + endyear + "/" + endmonth + "/" + rangedate;
		String term = dateRange + "[MDAT]+OR+" + dateRange
				+ "[PDAT]+AND+public[Filter]";
		String url = base + "esearch.fcgi?db=biosample&term=" + term
				+ "&usehistory=y&retmax=100000";

		try {
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'GET' request to URL : " + url);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				Pattern countPattern = Pattern.compile("<Count>(.*?)</Count>");
				Matcher matcher1 = countPattern.matcher(inputLine);
				Pattern queryKeyPat = Pattern
						.compile("<QueryKey>(.*?)</QueryKey>");
				Matcher matcher2 = queryKeyPat.matcher(inputLine);
				Pattern webEnvPat = Pattern.compile("<WebEnv>(.*?)</WebEnv>");
				Matcher matcher3 = webEnvPat.matcher(inputLine);
				Pattern idPat = Pattern.compile("<Id>(.*?)</Id>");
				Matcher matcher4 = idPat.matcher(inputLine);
				Pattern retmax = Pattern.compile("<RetMax>(.*?)<RetMax>");
				Matcher matcher5 = retmax.matcher(inputLine);
				if (matcher1.find() && !(results.containsKey("count"))) {
					results.put("count", matcher1.group(1));
				}
				if (matcher2.find()) {
					results.put("query_key", matcher2.group(1));
				}
				if (matcher3.find()) {
					results.put("WebEnv", matcher3.group(1));
				}
				if (matcher4.find()) {
					uids.add(matcher4.group(1));
				}
				if (matcher5.find()) {
					results.put("retMax", matcher5.group(1));
				}

			}
			in.close();

			// print result
			System.out.println(response.toString());

		} catch (MalformedURLException e) {
			System.out.println("The URL is not the correct one: " + url);
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Could not open connection");
			e.printStackTrace();
		}

	}

	/**
	 * Uses efetch to download a large data set in 500 record batches The data
	 * set must be stored on the History server. 
	 * use fetch by id instead
	 */

	public void efetchBatch() {
		String url = base + "efetch.fcgi?db=biosample";
		FileOutputStream fout = null;
		File file;
		try {
			file = new File(
					"/home/drashtti/Desktop/BioSamples/ncbi_uploader.xml");
			fout = new FileOutputStream(file);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (results.get("query_key") != null && results.get("WebEnv") != null) {
			url = url + "&querykey=" + results.get("query_key") + "&WebEnv="
					+ results.get("WebEnv") + "&retmode=xml&rettype=full";
		} else {
			System.out
					.println("WARNING: eFetch requires Query Key and WebEnv !!");
		}

		int retmax = 500;
		int last = 0;
		BufferedReader in = null;
		Integer count = Integer.parseInt(results.get("count")); // the size of
																// the dataset
		for (int retstart = 0; retstart < count; retstart = retmax + retstart) {
			try {
				Thread.sleep(delay);
				url = url + "&retstart=" + retstart + "&retmax=" + retmax;
				URL obj;
				try {
					obj = new URL(url);
					HttpURLConnection con = (HttpURLConnection) obj
							.openConnection();
					con.setRequestMethod("GET");
					in = new BufferedReader(new InputStreamReader(
							con.getInputStream()));
					int read = 0;
					byte[] bytes = new byte[1024];

					while ((read = in.read()) != -1) {
						fout.write(bytes, 0, read);
					}
					if (retstart + retmax > count) {
						last = count;
					} else {
						last = retstart + retmax;
					}
					int first = retstart + 1;
					System.out.println("Recieved records :" + (first - last));

				} catch (MalformedURLException e) {
					System.out.println("WARNING: Check the URL");
					e.printStackTrace();
				} catch (IOException e) {
					System.out
							.println("WARNING: Could not open HTTP connection");
					e.printStackTrace();
				}

			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}

		try {
			fout.close();
			in.close();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	
	/**
	 * This method downloads all the files from the biosamples
	 * database by id retrieved from the search method. 
	 */
	public void efetch_Id() {
		int count = Integer.parseInt(results.get("count")); // the size of the
															// dataset
		int retmax = Integer.parseInt(results.get("retMax")); // no. of ids
																// retrieved
		int uid_list = uids.size(); // no. of ids in the collected set
		if (count > retmax) {
			System.err
					.println("The number of entries modified/published in last week is greater than those retrieved i.e. 100,000");
		} else if (count != uid_list) {
			System.err
					.println("The number of Biosample IDs retrieved does not correspond");
		} else {
			String url = base + "efetch.fcgi?db=biosample";
			for (String uid : uids) {
				url = url + "&id=" + uid + "&retmode=xml&rettype=full";
				URL fetch;
				try {
					fetch = new URL(url);
					ReadableByteChannel rbc = Channels.newChannel(fetch
							.openStream());
					// FileOutputStream fos = new
					// FileOutputStream(uid+"_NCBI.xml");
					FileOutputStream fos = new FileOutputStream(make_file(uid));
					fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
				} catch (MalformedURLException e) {
					System.out.println("WARNING: CHECK THE URL");
					e.printStackTrace();
				} catch (IOException e) {
					System.out
							.println("WARNING: CANNOT COMPLETE THE CONNECTION");
					e.printStackTrace();
				}

			}

		}

	}

	
	/**
	 * This method makes sure that the naming of the 
	 * filesystem is standardized as previously used.
	 */
	public File make_file(String submissionID) {
		// NCBI biosamples
		File targetfile = new File("GNC");
		int i = 7;
		int groupsize = 3;
		while (i < submissionID.length()) {
			targetfile = new File(targetfile, submissionID.substring(0, i));
			i += groupsize;
		}
		return new File(targetfile, submissionID);
	}

}
