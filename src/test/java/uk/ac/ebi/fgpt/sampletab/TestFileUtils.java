package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils.FileFilterGlob;

import junit.framework.TestCase;


public class TestFileUtils extends TestCase {

    private Logger log = LoggerFactory.getLogger(getClass());

    public void testFileFilterRegex() {

        //first create a bunch of likely files
        
        //First
            //A1.txt
            //A2.txt
        //Second
            //A1.txt
            //A2.txt
        
        File first = new File("First");
        if (!first.exists()) first.mkdir();
        File firstA1 = new File(first, "A1.txt");
        File firstA2 = new File(first, "A2.txt");
        File second = new File("Second");
        if (!second.exists()) second.mkdir();
        File secondA1 = new File(second, "A1.txt");
        File secondA2 = new File(second, "A2.txt");
        try {
            org.apache.commons.io.FileUtils.touch(firstA1);
            org.apache.commons.io.FileUtils.touch(firstA2);
            org.apache.commons.io.FileUtils.touch(secondA1);
            org.apache.commons.io.FileUtils.touch(secondA2);
        } catch (IOException e) {
            log.error("unable to create files for testing");
            e.printStackTrace();
            fail();
        }
        
        
        //now check the results are what we expect
        String regex = ".*/A1\\.txt";
        log.info(regex+" : "+FileUtils.getMatchesRegex(regex));
    
    }

}
