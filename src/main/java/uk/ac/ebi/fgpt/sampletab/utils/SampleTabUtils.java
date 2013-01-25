package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class SampleTabUtils {
    private static Logger log = LoggerFactory.getLogger("uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils");

    //make sure this is kept in sync with uk.ac.ebi.fgpt.conan.process.biosd.AbstractBioSDProcess.getPathPrefix
    public static String getPathPrefix(String submissionId){
        if (submissionId.startsWith("GMS-")) return "imsr";
        else if (submissionId.startsWith("GAE-")) return "ae";
        else if (submissionId.startsWith("GPR-")) return "pride";
        else if (submissionId.startsWith("GVA-")) return "dgva";
        else if (submissionId.startsWith("GCR-")) return "coriell";
        else if (submissionId.startsWith("GEN-")) return "sra";
        else if (submissionId.startsWith("GEM-")){
            File targetfile = new File("GEM", submissionId.substring(0,7));
            return targetfile.getPath();
        }
        else if (submissionId.startsWith("GSB-")) return "GSB";
        else if (submissionId.equals("GEN")) return "encode";
        else if (submissionId.equals("G1K")) return "g1k";
        else if (submissionId.startsWith("GHM")) return "hapmap";
        else throw new IllegalArgumentException("Unable to get path prefix for "+submissionId);
    }
    
    public static boolean releaseInACentury(File sampletabFile) throws IOException, ParseException{
        SampleTabSaferParser parser = new SampleTabSaferParser();
        SampleData sd = parser.parse(sampletabFile);
        if (sd == null){
            log.error("Failed to parse "+sampletabFile);
            return false;
        } else if(sd.msi.submissionReleaseDate.before(new Date())) {
            //if its already public, then release it in 100 years
            Writer writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(sampletabFile));
                SampleTabWriter stwriter = new SampleTabWriter(writer);
                stwriter.write(sd);
            } catch (IOException e){
                throw e;
            } finally {
                if (writer != null){
                    try {
                        writer.close();
                    } catch (IOException e) {
                        //do nothing
                    }
                }
            }

            return true;
        } else {
            return false;
        }
    }
    
    public static boolean releaseInACentury(SampleData sd) {
        if (sd == null){
            throw new IllegalArgumentException("Must provide non-null SampleData");
        }
        if(sd.msi.submissionReleaseDate.before(new Date())) {
            //release it in 100 years
            Calendar cal = GregorianCalendar.getInstance();
            cal.set(Calendar.YEAR, cal.get(Calendar.YEAR)+100);
            sd.msi.submissionReleaseDate = cal.getTime();
            return true;
        }
        return false;
    }
    
    public static String generateSubmissionTitle(SampleData sd){
        /*
NUMBER SPECIES samples (from DATABASE)

NUMBER
    number of samples in group
SPECIES
    if all the same species latin (common) species name
    if multiple species then omit
         */
        
        for (SampleNode sample : sd.scd.getNodes(SampleNode.class)){
            //TODO FINISH ME!
        }
        
        return null;
        
        
    }
    
}
