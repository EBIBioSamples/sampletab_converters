package uk.ac.ebi.fgpt.sampletab;

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
    	magetabftpdownload.download(accession, accession);
    }

}
