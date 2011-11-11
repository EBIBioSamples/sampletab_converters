package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

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
