package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;

import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLFragmenter;
import uk.ac.ebi.fgpt.sampletab.utils.XMLFragmenter.ElementCallback;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBIFTPDriver extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    protected File outputDir;
    
    @Argument(required = true, index = 1, metaVar = "STARTDATE", usage = "Start date as YYYY/MM/DD")
    protected String minDateString;

    @Argument(required = true, index = 2, metaVar = "ENDDATE", usage = "End date as YYYY/MM/DD")
    protected String maxDateString;

	@Option(name = "--threads", aliases = { "-t" }, usage = "number of additional threads")
	protected int threads = 0;

	@Option(name = "--download", aliases = { "-d" }, usage = "downloadfile")
	protected File downloadFile = null;

	@Option(name = "--no-conan", usage = "do not trigger conan loads?")
	private boolean noconan = false;

	private SimpleDateFormat argumentDateFormat = new SimpleDateFormat("yyyy/MM/dd");
	
	private SimpleDateFormat xmlDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	private Logger log = LoggerFactory.getLogger(getClass());

	
	private ExecutorService pool = null;
	private LinkedList<Future<Void>> futures = new LinkedList<Future<Void>>();

	private Date fromDate = null;
	private Date toDate = null;

	private XMLFragmenter fragment;
	
	private class ElementCallable implements Callable<Void> {

		private final Element element;
		
		public ElementCallable(Element element){
			this.element = element;
		}
		
		@Override
		public Void call() throws Exception {
			//all check if this is worth processing are done in isBlockStart
			String accession = element.attributeValue("accession");
			String submission = "GNC-"+accession;
			log.info("Processing accession "+accession);			
			
			SampleData sd = null;
			try {
				sd = NCBIBiosampleRunnable.convert(element);
			} catch (ParseException e) {
				log.error("Unable to parse "+accession);
				return null;
			} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
				log.error("Unable to parse "+accession);
				return null;
			}
			
			if (sd == null) {
				return null;
			}
			
            File localOutDir = new File(outputDir, SampleTabUtils.getSubmissionDirPath(submission));
            localOutDir = localOutDir.getAbsoluteFile();
            localOutDir.mkdirs();
            File sampletabFile = new File(localOutDir, "sampletab.pre.txt");

	        // write back out
	        FileWriter out = null;
	        try {
	            out = new FileWriter(sampletabFile);
	        } catch (IOException e) {
	            log.error("Error opening " + sampletabFile, e);
	            return null;
	        }

	        Normalizer norm = new Normalizer();
	        norm.normalize(sd);

	        SampleTabWriter sampletabwriter = new SampleTabWriter(out);
	        try {
	            sampletabwriter.write(sd);
	        } catch (IOException e) {
	            log.error("Error writing " + sampletabFile, e);
	            return null;
	        } finally {
	        	if (sampletabwriter != null) {
		            try {
						sampletabwriter.close();
					} catch (IOException e) {
						//do nothing
					}
				}		        	
	        }

	        //trigger conan if appropriate
	        if (!noconan) {
	            try {
					ConanUtils.submit(sd.msi.submissionIdentifier, "BioSamples (other)");
				} catch (IOException e) {
					log.error("Problem starting conan for "+sd.msi.submissionIdentifier);
				}
	        }
	        //finish here
	        return null;
		}
		
	};
	
	private ElementCallback callback = new ElementCallback() {
		
		@Override
		public void handleElement(Element element) {
			Callable<Void> call = new ElementCallable(element);
			if (pool == null) {
				try {
					call.call();
				} catch (Exception e) {
					log.error("Problem handling element", e);
				}
			} else {
				Future<Void> f = pool.submit(call);
				futures.add(f);
				//stop the queue getting too large by checking it here
				checkQueue(threads*10);
			}
		}

		@Override
		public boolean isBlockStart(String uri, String localName,
				String qName, Attributes attributes) {
			//its not a biosample element, skip
			if (!qName.equals("BioSample")) {
				return false;
			}
			//its not public, skip
			if (!attributes.getValue("", "access").equals("public")) {
				return false;
			}
			//its an EBI biosample, or has no accession, skip
			if (attributes.getValue("", "accession") == null || attributes.getValue("", "accession").startsWith("SAME")) {
				return false;
			}
			//check the date compared to window
			String accession = attributes.getValue("", "accession");
			Date updateDate = null;
			try {
				updateDate = xmlDateTimeFormat.parse(attributes.getValue("", "last_update"));
			} catch (ParseException e1) {
				log.error("Unable to parse date "+attributes.getValue("", "last_update")+" on "+accession);
				return false;
			}
			Date releaseDate = null;
			try {
				releaseDate = xmlDateTimeFormat.parse(attributes.getValue("", "publication_date"));
			} catch (ParseException e1) {
				log.error("Unable to parse date "+attributes.getValue("", "last_update")+" on "+accession);
				return false;
			}
			
			Date latestDate = updateDate;
			if (releaseDate.after(latestDate)) {
				latestDate = releaseDate;
			}
			
			if (fromDate != null && latestDate.before(fromDate)) {
				return false;
			}

			if (toDate != null && latestDate.after(toDate)) {
				return false;
			}
			
			return true;
		}
	};


	public NCBIFTPDriver() {
		// dummy constructor
	}
	
	/**
	 * Do some initial setup that might throw exceptions
	 */
	public void setup() {

		try {
			fragment = XMLFragmenter.newInstance();
		} catch (ParserConfigurationException e) {
			log.error("Unable to create SAX parser", e);
			return;
		} catch (SAXException e) {
			log.error("Unable to create SAX parser", e);
			return;
		}
	}
	
	protected void doMain(String[] args) {
		super.doMain(args);

		setup();

		//convert data parameters into date objects
		try {
			fromDate = argumentDateFormat.parse(minDateString);
		} catch (ParseException e) {
			log.error("Unable to parse date " + minDateString);
			return;
		}
		try {
			toDate = argumentDateFormat.parse(maxDateString);
		} catch (ParseException e) {
			log.error("Unable to parse date " + maxDateString);
			return;
		}
		
		//create some threads if using threading
		if (threads > 0) {
			pool = Executors.newFixedThreadPool(threads);
		}
		
		//establish a connection to the FTP site
		NCBIFTP ncbiftp = new NCBIFTP();
		ncbiftp.setup("ftp.ncbi.nlm.nih.gov");

		//this is the remote filename we want
		String remoteFile = "/biosample/biosample_set.xml.gz";

		InputStream is = null;
		try {
			if (downloadFile == null) {
				is = ncbiftp.streamFromFTP(remoteFile);
			} else {
				is = ncbiftp.streamFromLocalCopy(remoteFile, downloadFile);
			}
			handleGZStream(is);
		} catch (IOException e) {
			log.error("Error accessing ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
			return;
		} catch (ParseException e) {
			log.error("Unable to read date", e);
			return;
		} catch (ParserConfigurationException e) {
			log.error("Unable to create SAX parser", e);
			return;
		} catch (SAXException e) {
			log.error("Unable to handle SAX", e);
			return;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		
		//make sure the queue is finished if using pooling
		checkQueue(0);
	}
	
	public void handleGZStream(InputStream inputStream) throws ParserConfigurationException, SAXException, IOException {
		handleStream(new GZIPInputStream(inputStream));
	}
	
	public void handleStream(InputStream inputStream) throws ParserConfigurationException, SAXException, IOException {
		fragment.handleStream(inputStream, "UTF-8", callback);	
	}

	public void checkQueue(int maxSize) {
		while (futures.size() > maxSize) {
			Future<Void> next = futures.removeFirst();
			try {
				next.get();
			} catch (InterruptedException e) {
				log.error("Problem handling element", e);
			} catch (ExecutionException e) {
				log.error("Problem handling element", e);
			}
		}
	}
	
	public static void main(String[] args) {
		new NCBIFTPDriver().doMain(args);

	}

}