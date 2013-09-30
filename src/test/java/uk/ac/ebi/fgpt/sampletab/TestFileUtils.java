package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;


public class TestFileUtils extends TestCase {

    private Logger log = LoggerFactory.getLogger(getClass());

    
    @Override
    public void setUp(){
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
        
    }
    
    @Override
    public void tearDown(){
        File first = new File("First");
        File firstA1 = new File(first, "A1.txt");
        File firstA2 = new File(first, "A2.txt");
        File second = new File("Second");
        File secondA1 = new File(second, "A1.txt");
        File secondA2 = new File(second, "A2.txt");
        firstA1.delete();
        firstA2.delete();
        first.delete();
        secondA1.delete();
        secondA2.delete();
        second.delete();
    }
    
    public void testFileFilterRegex() {
        
        
        //now check the results are what we expect
        List<File> FA1A2 = new ArrayList<File>();
        FA1A2.add(new File("First/A1.txt").getAbsoluteFile());
        FA1A2.add(new File("First/A2.txt").getAbsoluteFile());
        //assertEquals(FA1A2, FileUtils.getMatchesRegex("First/.*\\.txt"));
        assertEquals(FA1A2, FileUtils.getMatchesGlob("First/*.txt"));
        
        List<File> FA1 = new ArrayList<File>();
        FA1.add(new File("First/A1.txt").getAbsoluteFile());
        //assertEquals(FA1, FileUtils.getMatchesRegex("First/A1.txt"));
        assertEquals(FA1, FileUtils.getMatchesGlob("First/A1.txt"));

        List<File> FA1SA1 = new ArrayList<File>();
        FA1SA1.add(new File("First/A1.txt").getAbsoluteFile());
        FA1SA1.add(new File("Second/A1.txt").getAbsoluteFile());
        assertEquals(FA1SA1, FileUtils.getMatchesGlob("*/A1.txt"));
        //also test recursives while we are here
        
        assertEquals(FA1SA1, FileUtils.getRecursiveFiles("A1.txt"));
        
        List<File> FA1A2SA1A2 = new ArrayList<File>();
        FA1A2SA1A2.add(new File("First/A1.txt").getAbsoluteFile());
        FA1A2SA1A2.add(new File("First/A2.txt").getAbsoluteFile());
        FA1A2SA1A2.add(new File("Second/A1.txt").getAbsoluteFile());
        FA1A2SA1A2.add(new File("Second/A2.txt").getAbsoluteFile());
        assertEquals(FA1A2SA1A2, FileUtils.getMatchesGlob("*/A*.txt"));
        

        List<File> F = new ArrayList<File>();
        F.add(new File("First/").getAbsoluteFile());
        assertEquals(F, FileUtils.getMatchesGlob("Fir*/"));
        
        
        
    }

}
