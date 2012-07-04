package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class SampleTabUtils {
    private static Logger log = LoggerFactory.getLogger("uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils");

    public static File submissionDir;

    public static String getPathPrefix(String submissionId){
        if (submissionId.startsWith("GMS-")) return "imsr";
        else if (submissionId.startsWith("GAE-")) return "ae";
        else if (submissionId.startsWith("GPR-")) return "pride";
        else if (submissionId.startsWith("GVA-")) return "dgva";
        else if (submissionId.startsWith("GCR-")) return "coriell";
        else if (submissionId.startsWith("GEN-")) return "sra";
        else if (submissionId.equals("GEN")) return "encode";
        else if (submissionId.equals("G1K")) return "g1k";
        else if (submissionId.startsWith("GHM")) return "hapmap";
        else throw new IllegalArgumentException("Unable to get path prefix for "+submissionId);
    }
    
    public static File getSubmissionFile(String submissionId){
        File subdir = new File(submissionDir, getPathPrefix(submissionId));
        File subsubdir = new File(subdir, submissionId);
        File sampletabFile = new File(subsubdir, "sampletab.txt");
        return sampletabFile;
    }
    
    public static void releaseInADecade(File sampletabFile) throws IOException, ParseException{
        SampleTabSaferParser parser = new SampleTabSaferParser();
        SampleData sd = parser.parse(sampletabFile);
        if (sd == null){
            log.error("Failed to parse "+sampletabFile);
        } else {
            //release it in 10 years
            Calendar cal = GregorianCalendar.getInstance();
            cal.set(Calendar.YEAR, cal.get(Calendar.YEAR)+10);
            sd.msi.submissionReleaseDate = cal.getTime();
            Writer writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(sampletabFile));
                SampleTabWriter stwriter = new SampleTabWriter(writer);
                stwriter.write(sd);
            } catch (IOException e){
                e.printStackTrace();
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
        }
    }
}
