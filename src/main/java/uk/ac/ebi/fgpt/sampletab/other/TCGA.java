package uk.ac.ebi.fgpt.sampletab.other;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class TCGA extends AbstractDriver {

        @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output filename")
        private String outputFilename;

        @Option(name = "--clinical", usage = "input clinical filename")
        protected String inputClinicalFilename = null;

        @Option(name = "--biospecimen",  usage = "input biospecimen filename")
        protected String inputBioSpecimenFilename = null;
        
        @Option(name = "--abbreviation")
        protected String abbreviation = null;
        
        private Logger log = LoggerFactory.getLogger(getClass());
        private final SampleData st;

        public static void main(String[] args) {
            new TCGA().doMain(args);
        }
        
        private TCGA() {
            st = new SampleData();
        }
        
        private void doClinical() {
            if (inputClinicalFilename == null) {
                return;
            }
            File inputFile = new File(inputClinicalFilename);
            
            CSVReader reader = null;
            int linecount;
            String [] nextLine;
            String[] headers = null;
            try {
                reader = new CSVReader(new FileReader(inputFile), "\t".charAt(0));
                linecount = 0;
                while ((nextLine = reader.readNext()) != null) {
                    linecount += 1;
                    if (linecount % 10000 == 0){
                        log.info("processing line "+linecount);
                    }
                                    
                    if (headers == null || linecount == 0) {
                        headers = nextLine;
                    } else {
                        Map<String, String> line = new HashMap<String, String>();
                        for (int i = 0; i < nextLine.length; i++) {
                            line.put(headers[i], nextLine[i]);
                        }
                        SampleNode sample;
                        sample = st.scd.getNode(line.get("bcr_patient_barcode"), SampleNode.class);
                        if (sample == null) {
                            sample = new SampleNode(line.get("bcr_patient_barcode"));
                            st.scd.addNode(sample);
                        }
                    }
                }
                reader.close();
            } catch (FileNotFoundException e) {
                log.error("Error processing "+inputFile, e);
            } catch (IOException e) {
                log.error("Error processing "+inputFile, e);
            } catch (ParseException e) {
                log.error("Error building Sampledata", e);
            } finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        
        private void doBioSpecimen() {
            if (inputBioSpecimenFilename == null) {
                return;
            }
            File inputFile = new File(inputBioSpecimenFilename);
            
            CSVReader reader = null;
            int linecount;
            String [] nextLine;
            String[] headers = null;
            try {
                reader = new CSVReader(new FileReader(inputFile), "\t".charAt(0));
                linecount = 0;
                while ((nextLine = reader.readNext()) != null) {
                    linecount += 1;
                    if (linecount % 10000 == 0){
                        log.info("processing line "+linecount);
                    }
                                    
                    if (headers == null || linecount == 0) {
                        headers = nextLine;
                    } else {
                        Map<String, String> line = new HashMap<String, String>();
                        for (int i = 0; i < nextLine.length; i++) {
                            line.put(headers[i], nextLine[i]);
                        }
                        SampleNode sample;
                        sample = st.scd.getNode(line.get("bcr_patient_barcode"), SampleNode.class);
                        if (sample == null) {
                            sample = new SampleNode(line.get("bcr_patient_barcode"));
                            st.scd.addNode(sample);
                        }
                    }
                }
                reader.close();
            } catch (FileNotFoundException e) {
                log.error("Error processing "+inputFile, e);
            } catch (IOException e) {
                log.error("Error processing "+inputFile, e);
            } catch (ParseException e) {
                log.error("Error building Sampledata", e);
            } finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (IOException e) {
                    //do nothing
                }
            }
            
        }
        
        protected void doMain(String[] args) {
            super.doMain(args);
            
            if (abbreviation != null) {
                //no per-sample links, only per project
                st.msi.databases.add(new Database("The Cancer Genome Atlas", abbreviation, "https://tcga-data.nci.nih.gov/tcga/tcgaCancerDetails.jsp?diseaseType="+abbreviation));
            }
            
            //mark as reference layer
            st.msi.submissionReferenceLayer = true;
            
            doClinical();
            
            doBioSpecimen();

            //write output
            SampleTabWriter writer = null ; 
            File outputFile = new File(outputFilename);
            try {
                writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outputFile)));
                writer.write(st);
                writer.close();
            } catch (IOException e) {
                log.error("Error writing to "+outputFile, e);
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        //do nothing
                    }
                }
            }
        }
}
