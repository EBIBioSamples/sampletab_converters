package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
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

	private SimpleDateFormat ftpDateTimeFormat = new SimpleDateFormat("YYYYMMDDhhmmss");
	
	private SimpleDateFormat xmlDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	private Logger log = LoggerFactory.getLogger(getClass());


	private Date fromDate = null;
	private Date toDate = null;

	private XMLFragmenter fragment;
	
	private final List<String> accessions = new ArrayList<String>();
	
	private ElementCallback callback = new ElementCallback() {
		
		@Override
		public void handleElement(Element element) {
			//all check if this is worth processing are done in isBlockStart
			String accession = element.attributeValue("accession");
			String submission = "GNC-"+accession;
			accessions.add(accession);
			log.info("Processing accession "+accession);			
			
			SampleData sd = null;
			try {
				sd = NCBIBiosampleRunnable.convert(element);
			} catch (ParseException e) {
				log.error("Unable to parse "+accession);
				return;
			} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
				log.error("Unable to parse "+accession);
				return;
			}
			
			if (sd != null) {
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
		            return;
		        }

		        Normalizer norm = new Normalizer();
		        norm.normalize(sd);

		        SampleTabWriter sampletabwriter = new SampleTabWriter(out);
		        try {
		            sampletabwriter.write(sd);
		        } catch (IOException e) {
		            log.error("Error writing " + sampletabFile, e);
		            return;
		        } finally {
		        	if (sampletabwriter != null) {
			            try {
							sampletabwriter.close();
						} catch (IOException e) {
							//do nothing
						}
					}		        	
		        }
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
			if (attributes.getValue("", "access").equals("public")) {
				return false;
			}
			//its an EBI biosample, skip
			if (attributes.getValue("", "accession").startsWith("SAME")) {
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
	
	public void setup() {

		if (minDateString != null) {
			try {
				fromDate = argumentDateFormat.parse(minDateString);
			} catch (ParseException e) {
				log.error("Unable to parse date " + minDateString);
				return;
			}
		}
		
		if (maxDateString != null) {
			try {
				toDate = argumentDateFormat.parse(maxDateString);
			} catch (ParseException e) {
				log.error("Unable to parse date " + maxDateString);
				return;
			}
		}

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
		
		ExecutorService pool = null;
		if (threads > 0) {
			pool = Executors.newFixedThreadPool(threads);
		}
		
		FTPClient ftpClient = new FTPClient();
		String server = "ftp.ncbi.nlm.nih.gov";
		try {
			ftpClient.connect(server);
			ftpClient.login("anonymous", "");
			log.info("Connected to " + server + ".");
			log.info(ftpClient.getReplyString());

			// After connection attempt, check the reply code to verify success.
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				throw new RuntimeException("FTP connection failed");
			}
		} catch (SocketException e) {
			log.error("Unable to connect to ftp.ncbi.nlm.nih.gov", e);
			return;
		} catch (IOException e) {
			log.error("Unable to connect to ftp.ncbi.nlm.nih.gov", e);
			return;
		}

		String remoteFile = "/biosample/biosample_set.xml.gz";
		
		if (downloadFile == null) {
	
			InputStream is = null;
	
			try {
				is = ftpClient.retrieveFileStream(remoteFile);
				handleGZStream(is);
			} catch (ParserConfigurationException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} catch (SAXException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} catch (IOException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
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
		} else {
			FileOutputStream fileoutputstream = null;

			Date ftpModDate = null;
			try {
				ftpModDate = ftpDateTimeFormat.parse(ftpClient.getModificationTime(remoteFile));
			} catch (ParseException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} catch (IOException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			}
			
			boolean download = true;
			
			if (downloadFile.exists()) {
				Date modTime = new Date(downloadFile.lastModified());
				if (modTime.after(ftpModDate)) {
					download = false;
				}
			}
			
			if (download) {
				try {
					fileoutputstream = new FileOutputStream(downloadFile);
					ftpClient.retrieveFile(remoteFile, fileoutputstream);
					log.info("Downloaded " + remoteFile);
				} catch (IOException e) {
					log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
					return;
				} finally {
					if (fileoutputstream != null) {
						try {
							fileoutputstream.close();
						} catch (IOException e) {
							//do nothing
						}
					}
				}
			}

			FileInputStream fileinputstream = null;
			try {
				fileinputstream = new FileInputStream(downloadFile);
				handleGZStream(fileinputstream);
			} catch (FileNotFoundException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} catch (ParserConfigurationException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} catch (SAXException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} catch (IOException e) {
				log.error("Unable to process to ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", e);
				return;
			} finally {
				if (fileinputstream != null) {
					try {
						fileinputstream.close();
					} catch (IOException e) {
						//do nothing
					}
				}
			}
			
		}

	}
	
	public void handleGZStream(InputStream inputStream) throws ParserConfigurationException, SAXException, IOException {
		handleStream(new GZIPInputStream(inputStream));
	}
	
	public void handleStream(InputStream inputStream) throws ParserConfigurationException, SAXException, IOException {
		fragment.handleStream(inputStream, "UTF-8", callback);		
		Collections.sort(accessions);
		for (String accession : accessions) {
			System.out.println(accession);
		}		
	}

	public static void main(String[] args) {
		new NCBIFTPDriver().doMain(args);

	}

}
