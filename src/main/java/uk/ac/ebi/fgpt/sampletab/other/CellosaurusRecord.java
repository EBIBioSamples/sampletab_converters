package uk.ac.ebi.fgpt.sampletab.other;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;

public class CellosaurusRecord {
        /*
 ---------  ---------------------------     ----------------------
 Line code  Content                         Occurrence in an entry
 ---------  ---------------------------     ----------------------
 ID         Identifier (cell line name)     Once; starts an entry
 AC         Accession (CVCL_xxxx)           Once
 SY         Synonyms                        Optional; once
 DR         Cross-references                Optional; once or more
 WW         Web page URL                    Optional; once or more
 TS         Tissue (CALOHA ID)              Optional; once
 ST         Sampling tissue (CALOHA ID)     Optional; once
 CC         Comment                         Optional; once or more
 OX         Species of origin               Once or more
 HI         Hierarchy                       Optional; once or more
 OI         Originate from same individual  Optional; once or more
 SX         Sex of cell                     Optional; once
 CA         Category                        Optional; once
 //         Terminator                      Once; ends an entry
         */
        
    private String identifier;
    private String accession;
    private Set<String> synonyms = new HashSet<String>();
    private Set<String> xrefs = new HashSet<String>();
    private Set<URL> urls = new HashSet<URL>();
    private String tissue;
    private String samplingTissue;
    private Set<String> comments = new HashSet<String>();
    private Set<String> specieses = new HashSet<String>();
    private Set<String> heirarchy = new HashSet<String>();
    private Set<String> sameIndividual = new HashSet<String>();
    private String sex;
    private String category;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public static CellosaurusRecord createRecord(BufferedReader reader) throws IOException {
        CellosaurusRecord record = new CellosaurusRecord();
        
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("ID")) {
                record.identifier = line.substring(5);
            } else if (line.startsWith("AC")) {
                record.accession = line.substring(5);
            } else if (line.startsWith("SY")) {
                record.synonyms.add(line.substring(5));
            } else if (line.startsWith("DR")) {
                record.xrefs.add(line.substring(5));
            } else if (line.startsWith("WW")) {
                record.urls.add(new URL(line.substring(5)));
            } else if (line.startsWith("TS")) {
                record.tissue = line.substring(5); //TODO convert from caloha.obo reference to string
            } else if (line.startsWith("ST")) {
                record.samplingTissue = line.substring(5); //TODO convert from caloha.obo reference to string
            } else if (line.startsWith("CC")) {
                record.comments.add(line.substring(5));
            } else if (line.startsWith("OX")) {
                record.specieses.add(line.substring(5));
            } else if (line.startsWith("HI")) {
                record.heirarchy.add(line.substring(5));
            } else if (line.startsWith("OI")) {
                record.sameIndividual.add(line.substring(5));
            } else if (line.startsWith("SX")) {
                record.sex = line.substring(5);
            } else if (line.startsWith("CA")) {
                record.category = line.substring(5);
            } else if (line.startsWith("//")) {
                return record;
            }
        }
        return null;
    }


    public String getIdentifier() {
        return identifier;
    }


    public String getAccession() {
        return accession;
    }


    public Collection<String> getSynonyms() {
        return Collections.unmodifiableCollection(synonyms);
    }


    public Collection<String> getXRefs() {
        return Collections.unmodifiableCollection(xrefs);
    }


    public Collection<URL> getURLs() {
        return Collections.unmodifiableCollection(urls);
    }


    public String getTissue() {
        return tissue;
    }


    public String getSamplingTissue() {
        return samplingTissue;
    }


    public Collection<String> getComments() {
        return Collections.unmodifiableCollection(comments);
    }


    public Collection<String> getSpecieses() {
        return Collections.unmodifiableCollection(specieses);
    }


    public Collection<String> getHeirarchy() {
        return Collections.unmodifiableCollection(heirarchy);
    }


    public Collection<String> getSameIndividual() {
        return Collections.unmodifiableCollection(sameIndividual);
    }


    public String getSex() {
        return sex;
    }


    public String getCategory() {
        return category;
    }
}
