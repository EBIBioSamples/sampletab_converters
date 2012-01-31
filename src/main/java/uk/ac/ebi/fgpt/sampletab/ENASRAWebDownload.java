package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ENASRAWebDownload {
    // singlton instance
    private static ENASRAWebDownload instance = null;

    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    private ENASRAWebDownload() {
        // private constructor to prevent accidental multiple initialisations

    }

    public static ENASRAWebDownload getInstance() {
        if (instance == null) {
            instance = new ENASRAWebDownload();
        }

        return instance;
    }

    public String download(String accession, String outdir) {
        return this.download(accession, new File(outdir));
    }

    public String download(String accession, File outdir) {
        // TODO check accession is actually an ENA SRA study accession

        // create parent directories, if they dont exist
        File studyFile = new File(outdir.getAbsoluteFile(), "study.xml");
        if (!studyFile.getParentFile().exists()) {
            studyFile.getParentFile().mkdirs();
        }

        // setup the input as buffered characters
        // setup the output as a buffered file
        BufferedReader input;
        BufferedWriter output;
        String line;

        String url = "http://www.ebi.ac.uk/ena/data/view/" + accession + "&display=xml";

        log.info("Prepared for download.");
        try {
            input = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
            output = new BufferedWriter(new FileWriter(studyFile));
            // now go through each line in turn
            log.info("Starting study download...");
            while ((line = input.readLine()) != null) {
                output.write(line);
                output.write("\n");
            }
            log.info("Study download complete.");
            input.close();
            output.close();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }

        // now that the study is downloaded, it needs to be examined
        // to work out which samples to get

        try {
            Document studydoc = XMLUtils.getDocument(studyFile);
            Element root = studydoc.getRootElement();
            Set<String> sampleSRAAccessions = ENAUtils.getSamplesForStudy(root);

            // now there is a set of sample accessions they each need to be retrieved.
            log.info("Prepared for ENA SRA sample XML download.");
            for (String sampleSRAAccession : sampleSRAAccessions) {
                String sampleURL = "http://www.ebi.ac.uk/ena/data/view/" + sampleSRAAccession + "&display=xml";
                File sampleFile = new File(outdir.getAbsoluteFile(), sampleSRAAccession + ".xml");

                input = new BufferedReader(new InputStreamReader(new URL(sampleURL).openStream()));
                output = new BufferedWriter(new FileWriter(sampleFile));
                // now go through each line in turn
                while ((line = input.readLine()) != null) {
                    output.write(line);
                    output.write("\n");
                }
                input.close();
                output.close();
            }
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        } catch (DocumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        log.info("ENA SRA Sample download complete.");

        return null;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Must provide an ENA SRA study accession and an output directory.");
            return;
        }
        String accession = args[0];
        String outdir = args[1];

        ENASRAWebDownload downloader = ENASRAWebDownload.getInstance();
        String error = downloader.download(accession, outdir);
        if (error != null) {
            System.out.println("ERROR: " + error);
            System.exit(1);
        }

    }
}
