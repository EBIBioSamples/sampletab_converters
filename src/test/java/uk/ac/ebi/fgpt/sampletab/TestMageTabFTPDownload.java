package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;

import uk.ac.ebi.fgpt.sampletab.arrayexpress.MageTabFTPDownload;
import junit.framework.TestCase;


public class TestMageTabFTPDownload extends TestCase {
	
	private String accession = null;
	
	private MageTabFTPDownload magetabftpdownload= null;

    public void setUp() {
    	accession = "E-MEXP-986";
    	magetabftpdownload = MageTabFTPDownload.getInstance();
    }

    public void tearDown() {
    	accession = null;
    }

    public void testDownload() {
    	try {
            magetabftpdownload.download(accession, accession);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

}
