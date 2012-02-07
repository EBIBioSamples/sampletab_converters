package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

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
            firstA1.createNewFile();
            firstA2.createNewFile();
            secondA1.createNewFile();
            secondA2.createNewFile();
        } catch (IOException e) {
            log.error("unable to create files for testing");
            e.printStackTrace();
            fail();
        }
        
        
        //now check the results are what we expect
        List<File> target = new ArrayList<File>();
        target.add(new File("First/A1.txt"));
        target.add(new File("First/A2.txt"));
        String regex = "First/.*\\.txt";
        assertEquals(target, FileUtils.getMatchesRegex(regex));
        String glob = "First/*.txt";
        assertEquals(target, FileUtils.getMatchesGlob(glob));
    
    }

}
