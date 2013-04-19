package uk.ac.ebi.fgpt.sampletab.other;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.coode.owlapi.obo.parser.OBOParser;
import org.coode.owlapi.obo.parser.Token;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class Cellosaurus extends AbstractDriver {
    //see ftp://ftp.nextprot.org/pub/current_release/controlled_vocabularies/cellosaurus.txt


    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filename")
    protected File inputFile;

    @Argument(required=true, index=1, metaVar="OUTPUT", usage = "output File")
    private File outputFile;
    
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TermSource efo = new TermSource("EFO", "http://www.ebi.ac.uk/efo/", "2.23");
    private final TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy", null);

    private final Pattern organismPattern = Pattern.compile("NCBI_TaxID=([0-9]+); ! (.*)");

    private final Map<String, String> calohaMap = new HashMap<String,String>();
    
    private void setupCaloha() {
        URL caloha = null;
        try {
            caloha = new URL("ftp://ftp.nextprot.org/pub/current_release/controlled_vocabularies/caloha.obo");
        } catch (MalformedURLException e) {
            log.error("Bad URL", e);
        }
        if (caloha != null) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(caloha.openStream()));
                String line = null;
                String id = null;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("id: ")) {
                        id = line.substring(4).trim();
                    } else if (line.startsWith("name: ")) {
                        String label = line.substring(6).trim();
                        calohaMap.put(id, label);
                    }
                }
            } catch (IOException e) {
                log.error("Problem reading "+caloha, e);
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        //do nothing
                    }
                }
            }
        }
    }
    
    public static void main(String[] args) {
        new Cellosaurus().doMain(args);
    }    
    
    public void doMain(String[] args) {
        super.doMain(args);
        
        if (!inputFile.exists()) {
            log.error("File "+inputFile+" does not exist!");
            return;
        }

        setupCaloha();       
        
         
        SampleData sd = new SampleData();
        //some intial setup
        sd.msi.submissionTitle = "Cellosaurus: a controlled vocabulary of cell lines";
        sd.msi.submissionDescription = "A comprehensive thesaurus on cell lines, containing cell lines from over 30 resources, including ATCC, IMGT/HLA, CHEMBL, PubMed, MeSH, Brenda, EFO, MCCL, and CLO.";
        sd.msi.databases.add(new Database("Cellosaurus", "5.0", "ftp://ftp.nextprot.org/pub/current_release/controlled_vocabularies/cellosaurus.txt"));
        sd.msi.termSources.add(ncbitaxonomy);
        
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF8"));
            CellosaurusRecord record = null;
            while ((record = CellosaurusRecord.createRecord(br)) != null) {
                addRecordToSampleData(record, sd);
            }
        } catch (FileNotFoundException e) {
            log.error("Problem reading "+inputFile, e);
        } catch (IOException e) {
            log.error("Problem reading "+inputFile, e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        
        //post-processing
        //factor out same individual lines
        Collection<Set<String>> individuals = new LinkedList<Set<String>>();
        for (SampleNode s : sd.scd.getNodes(SampleNode.class)) {
            String name = s.getNodeName();
            Set<String> indSet = new HashSet<String>();
            indSet.add(name);
            individuals.add(indSet);
        }
        for (SampleNode s : sd.scd.getNodes(SampleNode.class)) {
            for (SCDNodeAttribute a:s.getAttributes()) {
                if (CommentAttribute.class.isInstance(a)) {
                    CommentAttribute ca = (CommentAttribute) a;
                    if (ca.type.equals("same individual")) {
                        //find the set we are in
                        //find the set they are in
                        // merge them
                        Set<String> usSet = null;
                        Set<String> themSet = null;
                        for (Set<String> testSet : individuals) {
                            if (testSet.contains(s.getNodeName())) {
                                usSet = testSet;
                            }
                            if (testSet.contains(ca.getAttributeValue())) {
                                themSet = testSet;
                            }
                        }
                        //comparison by id not equality
                        if (usSet != themSet) {
                            
                        }
                    }
                }
            }
        }
        
        //write output
        SampleTabWriter writer = null;
        log.info("writing "+outputFile);
        try {
            writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outputFile)));
            writer.write(sd);
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
    
    private void addRecordToSampleData(CellosaurusRecord record, SampleData sd) {
        log.trace("processing "+record.getIdentifier());
        SampleNode s = new SampleNode(record.getIdentifier());

        for (String synonym : record.getSynonyms()) {
            for (String subsynonym : synonym.split("; ")) {
                s.addAttribute(new CommentAttribute("synonym", subsynonym));
            }
        }
        if (record.getCategory() != null) {
            s.addAttribute(new MaterialAttribute(record.getCategory()));
        }
        if (record.getSex() != null) {
            s.addAttribute(new SexAttribute(record.getSex()));
        }
        for (String organism : record.getSpecieses()) {
            Matcher matcher = organismPattern.matcher(organism);
            boolean matched = matcher.find();
            if (!matched) {
                throw new RuntimeException("Unable to match organism pattern : "+organism);
            } else {
                s.addAttribute(new OrganismAttribute(matcher.group(2), ncbitaxonomy, new Integer(matcher.group(1))));   
            }
        }
        if (record.getTissue() != null) {
            //TODO map this CALOHA ID to text string
            String tissueName = calohaMap.get(record.getTissue());
            s.addAttribute(new CharacteristicAttribute("tissue", tissueName));
        }
        if (record.getSamplingTissue() != null) {
            //TODO map this CALOHA ID to text string
            String tissueName = calohaMap.get(record.getSamplingTissue());
            s.addAttribute(new CharacteristicAttribute("sampling tissue", tissueName));
        }
        

        for (String xref : record.getXRefs()) {
            //TODO something smarter
            String target = xref.split(";")[0];
            target = target.trim();
            String value = xref.split(";")[1];
            value = value.trim();
            s.addAttribute(new CommentAttribute(target+" cross-reference", value));
        }

        for (String xref : record.getHeirarchy()) {
            //TODO something smarter
            s.addAttribute(new CommentAttribute("heirarchy", xref));
        }

        for (String xref : record.getSameIndividual()) {
            //TODO something smarter
            s.addAttribute(new CommentAttribute("same individual", xref));
        }        
        
        for (URL webpage : record.getURLs()) {
            s.addAttribute(new CommentAttribute("URL", webpage.toExternalForm()));
        }
        
        for (String comment : record.getComments()) {
            s.addAttribute(new CommentAttribute("other", comment));
        }
        
        
        s.addAttribute(new DatabaseAttribute("Cellosaurus", record.getAccession(), null));
        
        try {
            sd.scd.addNode(s);
        } catch (ParseException e) {
            log.error("Problem adding "+record.getIdentifier(), e);
            return;
        }
    }

}
