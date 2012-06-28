package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.utils.MAGETABUtils;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonException;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonUtils;

public class Coriell {

    @Option(name = "-h", aliases = { "--help" }, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases = { "--input" }, usage = "input directory")
    private String inputFilename;

    @Option(name = "-o", aliases = { "--output" }, usage = "output directory")
    private String outputFilename;

    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new Coriell().doMain(args);
    }

    private class TabFile {
        public final String[][] data;
        public final List<String> headers;
        private String rownameColumn = null;

        public TabFile(File inputFile, String rownameColumn) throws IOException {
            InputStream is = new BufferedInputStream(new FileInputStream(inputFile));
            String[][] raw = MAGETABUtils.readTabDelimitedInputStream(is, "ISO8859_1", false, false);
            headers = Arrays.asList(raw[0]);
            data = Arrays.copyOfRange(raw, 1, raw.length);
            this.rownameColumn = rownameColumn;
            // check rownameColumn has unique values
            int headerIndex = headers.indexOf(this.rownameColumn);
            Set<String> rownames = new HashSet<String>();
            for (String[] row : data) {
                String value = row[headerIndex];
                if (rownames.contains(value)) {
                    throw new IllegalArgumentException("Rowname column " + this.rownameColumn
                            + " contains non-unique value " + value);
                }
                rownames.add(value);
            }
        }

        public TabFile(File inputFile) throws IOException {
            InputStream is = new BufferedInputStream(new FileInputStream(inputFile));
            String[][] raw = MAGETABUtils.readTabDelimitedInputStream(is, "ISO8859_1", false, false);
            headers = Arrays.asList(raw[0]);
            data = Arrays.copyOfRange(raw, 0, raw.length);
        }
    }

    public void doMain(String[] args) {

        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        File inputFile = new File(inputFilename);

        TabFile masterProductList = null;
        TabFile sampleDisplay = null;
        TabFile omimDiag = null;
        try {
            masterProductList = new TabFile(new File(inputFile, "master_product_list.txt"), "Inventory_id");
            sampleDisplay = new TabFile(new File(inputFile, "sample_display.txt"));
            omimDiag = new TabFile(new File(inputFile, "omim_diag.txt"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(2);
            return;
        }

        SampleData st = new SampleData();

        Set<String> cellLineNames = new HashSet<String>();
        Set<String> dnaNames = new HashSet<String>();

        for (String[] row : masterProductList.data) {
            String typeCode = row[1];
            if (typeCode.equals("CC")
                    && (row[10].length() > 0) // is in a collection
                ) {
                String name = row[0];
                //some entries in masterProductList are named with the expansion
                //lot added after a dash
                if (name.contains("-")){
                    name = name.split("-")[0];
                }
                //only create one entry for each cell line at this point.
                //multiple expansion lots are added later
                if (cellLineNames.contains(name)) {
                    //dont print an error where a name has had cell line expansion lot split out
                    if (name.equals(row[0])) {
                        log.warn("Duplicate cell line name " + name);
                    }
                } else {
                    //find the number of expansion lots of this cell line that are available
                    Set<String> expansionLots = new HashSet<String>();
                    for (String[] sampleDisplayRow : sampleDisplay.data ){
                        if (sampleDisplayRow[0].equals(name)){
                            if (expansionLots.contains(sampleDisplayRow[1])){
                                log.warn("Duplicate cell line expansion name " + name+"-"+sampleDisplayRow[1]);   
                            }
                            expansionLots.add(sampleDisplayRow[1]);
                        }
                    }
                    if (expansionLots.size() == 0){
                        log.warn("No cell line expansion lots for " + name);
                    }
                    //for each avaliable expansion lot create a sample
                    //TODO relate these together
                    for (String expansionLot : expansionLots) {
                        String[] sampleDisplayRow = null;
                        for (String[] testRow : sampleDisplay.data ){
                            if (testRow[0].equals(name) && testRow[1].equals(expansionLot)){
                                sampleDisplayRow = testRow;
                                break;
                            }
                        }
                        if (sampleDisplayRow == null) {
                            log.warn("Unable to find sample_display.txt row for "+name+"-"+expansionLot);
                            continue;
                        }
                        SampleNode sample = new SampleNode();
    
                        if (expansionLots.size() == 1){
                            sample.setNodeName(name);
                        } else {
                            sample.setNodeName(name+"-"+expansionLot);
                        }
    
                        // TODO format description better than all-caps
                        sample.setSampleDescription(row[11]);
    
                        MaterialAttribute ma = new MaterialAttribute("cell line");
                        ma.setTermSourceREF("EFO");
                        ma.setTermSourceID("http://www.ebi.ac.uk/efo/EFO_0000322");
                        sample.addAttribute(ma);
                        
                        String taxname = sampleDisplayRow[6]+" "+sampleDisplayRow[5];
                        taxname = taxname.trim();
                        if (taxname.length() > 0){
                            OrganismAttribute oa = new OrganismAttribute(taxname);
                            try {
                                int taxid = TaxonUtils.findTaxon(oa.getAttributeValue());
                                oa.setTermSourceREF("NCBI Taxonomy");
                                oa.setTermSourceIDInteger(taxid);
                            } catch (TaxonException e){
                                log.warn("unable to find taxid for "+oa.getAttributeValue());
                            }
                            sample.addAttribute(oa);
                        }
                        
                        if (sampleDisplayRow[12].length() > 0){
                            SexAttribute a = new SexAttribute(sampleDisplayRow[12].toLowerCase());
                            sample.addAttribute(a);
                        }
                        
                        sample.addAttribute(new CharacteristicAttribute("Cell Type", sampleDisplayRow[3]));
                        
                        sample.addAttribute(new CharacteristicAttribute("Organism Part", sampleDisplayRow[4]));
                        
                        if (sampleDisplayRow[8].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Ethnicity", sampleDisplayRow[8]));
                        
                        if (sampleDisplayRow[9].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Geographic Origin", sampleDisplayRow[9]));
                        
                        if (sampleDisplayRow[10].length() > 0) {
                            CharacteristicAttribute age = new CharacteristicAttribute("Age", sampleDisplayRow[10]);
                            if (sampleDisplayRow[11].length() > 0) {
                                age.unit = new UnitAttribute();
                                age.unit.type = "Time Unit";
                                if (sampleDisplayRow[11].equals("YR")){
                                    age.unit.setAttributeValue("year");
                                    age.unit.setTermSourceREF("EFO");
                                    age.unit.setTermSourceID("http://www.ebi.ac.uk/efo/EFO_0001725");
                                } else if (sampleDisplayRow[11].equals("WK")) {
                                    age.unit.setAttributeValue("week");
                                    age.unit.setTermSourceREF("EFO");
                                    age.unit.setTermSourceID("http://www.ebi.ac.uk/efo/EFO_0001788");
                                } else if (sampleDisplayRow[11].equals("FW")) {
                                    age.unit.setAttributeValue("fetal week");
                                    //TODO set development stage accordingly
                                } else if (sampleDisplayRow[11].equals("DA")) {
                                    age.unit.setAttributeValue("day");
                                    age.unit.setTermSourceREF("EFO");
                                    age.unit.setTermSourceID("http://www.ebi.ac.uk/efo/EFO_0001789");
                                }else if (sampleDisplayRow[11].equals("MO")) {
                                    age.unit.setAttributeValue("month");
                                    age.unit.setTermSourceREF("EFO");
                                    age.unit.setTermSourceID("http://purl.org/obo/owl/UO#UO:0000035");
                                } else {
                                    age.unit.setAttributeValue(sampleDisplayRow[11]);
                                }
                            }
                            sample.addAttribute(age);
                        }
                        
                        if (sampleDisplayRow[13].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Clinically Affected Status", sampleDisplayRow[13]));
                        if (sampleDisplayRow[14].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Genetic Status", sampleDisplayRow[14]));
                        

                        if (row[12].length() > 0){
                            for (String[] testRow : omimDiag.data ){
                                if (testRow[2].equals(row[12])){
                                    CharacteristicAttribute a = new CharacteristicAttribute("Disease State", testRow[1]);
                                    a.setTermSourceREF("OMIM");
                                    a.setTermSourceID("http://omim.org/entry/"+testRow[0]);
                                    sample.addAttribute(a);
                                    break;
                                }
                            }
                            
                        }
                        
                        
                        if (sampleDisplayRow[15].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Family Relationship", sampleDisplayRow[15]));
                        if (sampleDisplayRow[16].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Family Identifier", sampleDisplayRow[16]));
                        if (sampleDisplayRow[17].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Family Member Identifier", sampleDisplayRow[17]));
                        
                        if (sampleDisplayRow[18].length() > 0)
                            sample.addAttribute(new CharacteristicAttribute("Clinical History", sampleDisplayRow[18]));
                        
                        if (sampleDisplayRow.length > 31 && sampleDisplayRow[31].length() > 0)
                            sample.addAttribute(new CommentAttribute("Transformation Type", sampleDisplayRow[31]));
                        
                        sample.addAttribute(new CommentAttribute("Collection", sampleDisplayRow[2]));
                        
                        try {
                            st.scd.addNode(sample);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                    //dont store this cell line again
                    cellLineNames.add(name);
                }
            }
        }
        
        for (String[] row : masterProductList.data) {
            String typeCode = row[1];
            if (typeCode.equals("DNA")
                    && (row[10].length() > 0) // is in a collection
                ) {
                String name = row[0];
                if (dnaNames.contains(name)) {
                    log.error("Duplicate DNA name " + name);
                } else if (cellLineNames.contains(name)) {
                    // name is both a cell line and a DNA sample
                    // do nothing
                } else {

                    SampleNode sample = new SampleNode();

                    sample.setNodeName(name);

                    // TODO format description better than all-caps
                    sample.setSampleDescription(row[11]);

                    sample.addAttribute(new CommentAttribute("Collection", row[10]));

                    sample.addAttribute(new MaterialAttribute("DNA"));
                    try {
                        st.scd.addNode(sample);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        return;
                    }
                    
                    String cellLineName = name;
                    //these taken from collection_type.txt
                    if (name.startsWith("NG")) {
                        cellLineName = cellLineName.replace("NG", "AG");
                    } else if (name.startsWith("NU")) {
                        cellLineName = cellLineName.replace("NU", "AU");
                    } else if (name.startsWith("NE")) {
                        cellLineName = cellLineName.replace("NE", "DA");
                    } else if (name.startsWith("NA")) {
                        cellLineName = cellLineName.replace("NA", "GM");
                    }
                    
                    SampleNode cellLine = st.scd.getNode(cellLineName, SampleNode.class);
                    if (cellLine != null){
                        sample.addParentNode(cellLine);
                        cellLine.addChildNode(sample);
                    }
                    
                    //dont store this dna sample again
                    dnaNames.add(name);
                }
            }
        }

        
        //now break it up into separate parts for each collection
        Map<String, String> collections = new HashMap<String, String>();
        collections.put("ADA Repository", "GCR-ada");
        collections.put("Autism", "GCR-autism");
        collections.put("COHORT Project", "GCR-cohort");
        collections.put("Leiomyosarcoma", "GCR-leiomyosarcoma");
        collections.put("NHGRI Sample Repository for Human Genetic Research", "GCR-nhgri");
        collections.put("NIA Aging Cell Culture Repository", "GCR-nia");
        collections.put("NIAID - USIDnet", "GCR-niaid");
        collections.put("NIGMS Human Genetic Cell Repository", "GCR-nigms");
        collections.put("NINDS Repository", "GCR-ninds");
        collections.put("Wistar Collection", "GCR-winstar");
        collections.put("Yerkes Primates", "GCR-yerkes");
        collections.put("Integrated Primate Biomaterials Resource", "GCR-primate");
        //collections we dont use yet?
        //collections.put("Centers for Disease Control and Prevention Repository", "GCR-cdc");
        //collections.put("CHDI Foundation, Inc. Collection", "GCR-chdi");
        //collections.put("Neonatal Foreskin Epidermal Keratinocytes Collection", "GCR-nfek");

        //hard-code URLs abnd descriptions for each collection
        //taken from http://ccr.coriell.org/Sections/Collections/?SId=4
        Map<String, String> collectionURLs = new HashMap<String, String>();
        collectionURLs.put("GCR-ada", "http://ccr.coriell.org/Sections/Collections/ADA/?SsId=12");
        collectionURLs.put("GCR-nigms", "http://ccr.coriell.org/Sections/Collections/NIGMS/?SsId=8");
        collectionURLs.put("GCR-ninds", "http://ccr.coriell.org/Sections/Collections/NINDS/?SsId=10");
        collectionURLs.put("GCR-nia", "http://ccr.coriell.org/Sections/Collections/NIA/?SsId=9");
        collectionURLs.put("GCR-nhgri", "http://ccr.coriell.org/Sections/Collections/NHGRI/?SsId=11");
        collectionURLs.put("GCR-autism", "http://ccr.coriell.org/Sections/Collections/AUTISM/?SsId=13");
        collectionURLs.put("GCR-primate", "http://ccr.coriell.org/Sections/Collections/IPBIR/?SsId=18");
        collectionURLs.put("GCR-chri", "http://ccr.coriell.org/Sections/Collections/CHDI/?SsId=45");
        //collectionURLs.put("GCR-usidnet", "http://ccr.coriell.org/Sections/Collections/USIDNET/?SsId=15");
        //collectionURLs.put("GCR-cdc", "http://ccr.coriell.org/Sections/Collections/CDC/?SsId=16");
        collectionURLs.put("GCR-leiomyosarcoma", "http://ccr.coriell.org/Sections/Collections/LMS/?SsId=17");
        collectionURLs.put("GCR-cohort", "http://ccr.coriell.org/Sections/Collections/COHORT/?SsId=44");
        collectionURLs.put("GCR-yerkes", "http://ccr.coriell.org/Sections/Collections/YERKES/?SsId=66");
        //collectionURLs.put("GCR-areds", "http://ccr.coriell.org/Sections/Collections/AREDS/?SsId=68");
        collectionURLs.put("GCR-winstar", "http://ccr.coriell.org/Sections/Collections/Wistar/?SsId=74");
        //collectionURLs.put("GCR-huref", "http://ccr.coriell.org/Sections/Collections/HuRef/?SsId=78");

        Map<String, String> collectionDescriptions = new HashMap<String, String>();
        collectionDescriptions.put("GCR-nigms", "The Human Genetic Cell Repository, sponsored by the National Institute of General Medical Sciences, provides scientists around the world with resources for cell and genetic research. The samples include highly characterized cell lines and high quality DNA. Repository samples represent a variety of disease states, chromosomal abnormalities, apparently healthy individuals and many distinct human populations.");
        collectionDescriptions.put("GCR-ninds", "The National Institute of Neurological Disorders and Stroke is committed to gene discovery, as a strategy for identifying the genetic causes and correlates of nervous system disorders. The NINDS Human Genetics DNA and Cell Line Repository banks samples from subjects with cerebrovascular disease, epilepsy, motor neuron disease, Parkinsonism, and Tourette Syndrome, as well as controls.");
        collectionDescriptions.put("GCR-nia", "Sponsored by the National Institute on Aging (NIA), the AGING CELL REPOSITORY, is a resource facilitating cellular and molecular research studies on the mechanisms of aging and the degenerative processes associated with it. The cells in this resource have been collected over the past three decades using strict diagnostic criteria and banked under the highest quality standards of cell culture. Scientists use the highly-characterized, viable, and contaminant-free cell cultures from this collection for research on such diseases as Alzheimer disease, progeria, Parkinsonism, Werner syndrome, and Cockayne syndrome.");
        collectionDescriptions.put("GCR-nhgri", "The National Human Genome Research Institute (NHGRI) led the National Institutes of Health's (NIH) contribution to the International Human Genome Project, which had as its primary goal the sequencing of the human genome. This project was successfully completed in April 2003. Now, the NHGRI's mission has expanded to encompass a broad range of studies aimed at understanding the structure and function of the human genome and its role in health and disease.");
        collectionDescriptions.put("GCR-ada", "The purpose of the American Diabetes Association (ADA), GENNID Study (Genetics of non-insulin dependent diabetes mellitus, NIDDM) is to establish a national database and cell repository consisting of information and genetic material from families with well-documented NIDDM. The GENNID Study will provide investigators with the information and samples necessary to conduct genetic linkage studies and locate the genes for NIDDM.");
        collectionDescriptions.put("GCR-autism", "The State of New Jersey funded the initiation of a genetic resource to support the study of autism in families where more than one child is affected or where one child is affected and one demonstrates another significant and related developmental disorder. This resource now receives continuing support from the Coriell Institute for Medical Research. An open bank of anonymously collected materials documented by a detailed clinical diagnosis forms the basis of this growing database of information about the disease.");
        collectionDescriptions.put("GCR-primate", "The purpose of the IPBIR - Integrated Primate Biomaterials and Information Resource is to assemble, characterize, and distribute high-quality DNA samples of known provenance with accompanying demographic, geographic, and behavioral information in order to stimulate and facilitate research in primate genetic diversity and evolution, comparative genomics, and population genetics.");
        collectionDescriptions.put("GCR-chri", "HD Community BioRepository is a secure, centralized repository that stores and distributes quality-controlled, reliable research reagents. Huntingtin DNAs are now available and antibodies, antigenic peptides, cell lines, and hybridomas will be added soon.");
        //collectionDescriptions.put("GCR-usidnet", "The USIDNET DNA and Cell Repository has been established as part of an NIH-funded program - the US Immunodeficiency Network (www.usidnet.org) - to provide a resource of DNA and functional lymphoid cells obtained from patients with various primary immunodeficiency diseases. These uncommon disorders include patients with defects in T cell, B cell and/or granulocyte function as well as patients with abnormalities in antibodies/immunoglobulins, complement and other host defense mechanisms.");
        //collectionDescriptions.put("GCR-cdc", "The Genetic Testing Reference Material Coordination Program of the Centers for Disease Control and Prevention (CDC) and the Coriell Institute for Medical Research announce the availability of samples derived from transformed cell lines for use in molecular genetic testing. The DNA samples prepared from these reference cell lines are available through the Coriell Cell Repositories. Diseases include cystic fibrosis (CF), 5' 10' methylenetetrahydrofolate reductase deficiency (MTHFR), HFE-associated hereditary hemochromatosis, Huntington disease (HD), fragile X syndrome, Muenke syndrome, connexin 26-associated deafness, and alpha-thalassemia.");
        collectionDescriptions.put("GCR-leiomyosarcoma", "The Leiomyosarcoma Cell and DNA Repository has been established with an award from the National Leiomyosarcoma Foundation. This foundation provides leadership in supporting research of Leiomyosarcoma, improving treatment outcomes of those affected by this disease as well as fostering awareness in the medical community and general public.");
        collectionDescriptions.put("GCR-cohort", "The Cooperative Huntington's Observational Trial Repository has been established as a resource for the discovery of information related to Huntington's disease and its causes, progressioin, treatments, and possible cures. This is a growing bank for DATA and SPECIMENS to accelerate research on Huntington's disease.");
        collectionDescriptions.put("GCR-yerkes", "The Yerkes National Primate Research Center of Emory University is an international leader in biomedical and behavioral research. For more than seven decades, the Yerkes Research Center has been dedicated to advancing scientific understanding of primate biology, behavior, veterinary care and conservation, and to improving human health and well-being.");
        //collectionDescriptions.put("GCR-areds", "The Age-Related Eye Disease Study was designed to learn about macular degeneration and cataract, two leading causes of vision loss in older adults. The study looked at how these two diseases progress and what their causes may be. In addition, the study tested certain vitamins and minerals to find out if they can help to prevent or slow these diseases. Participants in the study did not have to have either disease. (Enrollment was completed in January 1998.) Eleven medical centers in the United States took part in the study, and more than 4,700 people across the country were enrolled in AREDS. The study was supported by the National Eye Institute, part of the Federal government's National Institutes of Health. The clinical trial portion of the study also received support from Bausch & Lomb Pharmaceuticals and was completed in October 2001. Learn about the results of the clinical trial on the National Eye Institute's website: http://www.nei.nih.gov/amd/.");
        collectionDescriptions.put("GCR-winstar", "The Wistar Institute collection at Coriell contains cell lines that have been developed by Wistar scientists. These materials are offered for non-commercial research conducted by universities, government agencies and academic research centers. The Wistar Institute collection currently contains a group of hybridomas that produce monoclonal antibodies that are useful in influenza research and vaccine development. Melanoma cell lines, derived from patients with a wide range of disease ranging from mild dysplasia to advanced metastatic cancer, will be added shortly. More information on The Wistar Institute, its research and scientists can be found at www.wistar.org.");
        //collectionDescriptions.put("GCR-huref", "The Human Reference Genetic Material Repository makes available DNA from a single individual, J. Craig Venter, whose genome has been sequenced and assembled. The DNA samples are prepared from a lymphoblastoid cell line established at Coriell Cell Repositories from a sample of peripheral blood. The DNA samples are available in 50 microgram aliquots. The lymphoblastoid cell line is not available for distribution..");
        
        for (String collection : collections.keySet()){
            File outdir = new File(outputFilename, collections.get(collection));
            File outfile = new File(outdir, "sampletab.pre.txt");
            outdir.mkdirs();
            
            log.info("Writing to "+outfile);
            
            SampleData toWriter = new SampleData();
            
            for (SampleNode sample : st.scd.getNodes(SampleNode.class)){
                for (SCDNodeAttribute attr : sample.getAttributes()){
                    if (attr.getAttributeValue().equals(collection)){
                        try {
                            toWriter.scd.addNode(sample);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return;
                        }
                        
                    }
                }
            }
            toWriter.msi.submissionTitle = "Coriell Cell Repositories - "+collection;
            toWriter.msi.submissionIdentifier = collections.get(collection);
            toWriter.msi.submissionDescription = "The Coriell Cell Repositories provide essential research reagents to the scientific community by establishing, verifying, maintaining, and distributing cell cultures and DNA derived from cell cultures. These collections, supported by funds from the National Institutes of Health (NIH) and several foundations, are extensively utilized by research scientists around the world.";
            toWriter.msi.submissionDescription += " "+collectionDescriptions.get(collections.get(collection));
            toWriter.msi.submissionReferenceLayer = true;
            toWriter.msi.submissionUpdateDate = new Date();
            toWriter.msi.submissionReleaseDate = new Date();
            
            Organization coriellOrganization = new Organization("Coriell Institute", "403 Haddon Avenue, New Jersey, 08103, USA", null, "http://ccr.coriell.org/", "submitter");
            toWriter.msi.organizations.add(coriellOrganization);
            Organization ebiOrganization = new Organization("EBI", "Wellcome Trust Genome Campus, Hinxton, Cambridge, CB10 1SD, UK", null, "http://www.ebi.ac.uk/", "curator");
            toWriter.msi.organizations.add(ebiOrganization);
            
            TermSource efo = new TermSource("EFO", "http://www.ebi.ac.uk/efo/", "2.23");
            toWriter.msi.termSources.add(efo);
            TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy", null);
            toWriter.msi.termSources.add(ncbitaxonomy);
            TermSource omim = new TermSource("OMIM", "http://omim.org", null);
            toWriter.msi.termSources.add(omim);
            
            Database coriellDatabase = new Database("Coriell Cell Repositories", collectionURLs.get(collections.get(collection)), null);
            toWriter.msi.databases.add(coriellDatabase);
            
            CoriellFamily family = new CoriellFamily();
            family.process(toWriter);
            
            SampleTabWriter writer = null;
            try {
                writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outfile)));
                writer.write(toWriter);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e1) {
                        // do nothing
                    }
                }
            }
        }
        
    }
}
