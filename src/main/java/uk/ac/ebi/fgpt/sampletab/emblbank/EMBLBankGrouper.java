package uk.ac.ebi.fgpt.sampletab.emblbank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.TaxonException;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonUtils;
import au.com.bytecode.opencsv.CSVReader;

public class EMBLBankGrouper {
    
    private final File groupFile;
    private final Map<String, Set<String>> groupMap;
    private final boolean bar;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public EMBLBankGrouper(File groupFile, Map<String, Set<String>> groupMap, boolean bar) {
        this.groupFile = groupFile;
        this.groupMap = groupMap;
        this.bar = bar;
    }
    
    public List<String> getGroupIdentifiers(String[] line, EMBLBankHeaders headers) {

        List<String> identifiers = new ArrayList<String>();
        
        String projString = null;
        if (headers.projheaderindex > 0 && headers.projheaderindex < line.length){
            projString = line[headers.projheaderindex].trim();
        }
        if (projString != null && projString.length() > 0){
            for(String projID : projString.split(",")){
                projID = projID.trim();
                if (projID.length() > 0){
                    identifiers.add(projID);
                }
            }
        } else {
            String pubString = null;
            if (headers.pubheaderindex > 0 && headers.pubheaderindex < line.length){
                pubString = line[headers.pubheaderindex];
                for(String pubID : pubString.split(",")){
                    pubID = pubID.trim();
                    if (pubID.length() > 0){
                        identifiers.add(pubID);
                    }
                }
            }
        }
        //no publications, fall back to identified by
        if (identifiers.size() == 0){
            if (headers.identifiedbyindex > 0 && headers.identifiedbyindex < line.length){
                String identifiedby = line[headers.identifiedbyindex];
                identifiedby = identifiedby.replaceAll("[^\\w]", "");
                
                if (identifiedby.length() > 0){
                    identifiers.add(identifiedby);
                }
            }
        }
        //no publications, fall back to collected by
        if (identifiers.size() == 0){
            if (headers.collectedbyindex > 0 && headers.collectedbyindex < line.length){
                String collectedby = line[headers.collectedbyindex];
                collectedby = collectedby.replaceAll("[^\\w]", "");
                
                if (collectedby.length() > 0){
                    identifiers.add(collectedby);
                }
            }
        }
        //still nothing, have to use species
        //TODO maybe abstract this a level or two up the taxonomy?
        if (bar && identifiers.size() == 0){
            String organism = null;
            
            //use division, not species
            try {
                organism = TaxonUtils.getDivisionOfID(new Integer(line[headers.taxidindex]));
                organism.replaceAll(" ", "-");
                organism.replaceAll("[^\\w_]", "");
                while (organism.contains("--")){
                    organism.replaceAll("--", "-");
                }
            } catch (NumberFormatException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (TaxonException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            if (organism != null && organism.length() > 0){
                organism = "taxon-"+organism;
                identifiers.add(organism);
            }
        }
        
        if (identifiers.size() == 0){
            //log.debug(line[0]+" has  no identifiers.");
            identifiers.add(line[0]);
        }
        return identifiers;
    }

    private void writeGrouping() {
        try {
            Writer writer = new BufferedWriter(new FileWriter(groupFile));
            for (String id : groupMap.keySet()){
                writer.write(id);
                for (String accession : groupMap.get(id)){
                    writer.write("\t");
                    writer.write(accession);
                }
                writer.write("\n");
            }
        } catch (IOException e) {
            log.error("unable to write grouping", e);
        }
    }
    
    public void process(File inputFile) throws IOException {

        CSVReader reader = null;
        
        EMBLBankHeaders headers = null;
        
        int linecount;
        String [] nextLine;
        
        try {
            reader = new CSVReader(new FileReader(inputFile), "\t".charAt(0));
            linecount = 0;
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                                
                if (headers == null || linecount == 0){
                    headers = new EMBLBankHeaders(nextLine);
                } else {
                    if (nextLine.length > headers.size()){
                        log.warn("Line longer than headers "+linecount+" ( "+nextLine.length+" vs "+headers.size()+" )");
                    }
                
                    String accession = nextLine[0].trim();
                    log.debug("First processing "+accession);
                    
                    for (String id : getGroupIdentifiers(nextLine, headers)){

                        if(!groupMap.containsKey(id)){
                            groupMap.put(id, new HashSet<String>());
                        }
                        groupMap.get(id).add(accession);
                    }                    
                }
            }
            reader.close();
        } finally {
            try {
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e){
                //do nothing
            }
        }

        log.info("First pass complete");
        log.info("No. of groups = "+groupMap.size());

        if (groupFile != null){
            writeGrouping();
        }
    }

    public void readGrouping(int groupIndex, int groupOffset) {
        CSVReader reader = null;
        String [] nextLine;
        int linecount = 0;
        
        try {
            reader = new CSVReader(new FileReader(groupFile), "\t".charAt(0));
            while ((nextLine = reader.readNext()) != null) {
                if (linecount % groupIndex == groupOffset) {
                    Set<String> group = new HashSet<String>();
                    for (int i = 1; i < nextLine.length; i++) {
                        group.add(nextLine[i].trim());
                    }
                    groupMap.put(nextLine[0], group);
                }
                linecount += 1;
            }
            reader.close();
        } catch (IOException e) {
            log.error("Problem reading "+groupFile, e);
        }
    }
}
