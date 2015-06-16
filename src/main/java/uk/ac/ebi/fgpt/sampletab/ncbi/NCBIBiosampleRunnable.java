package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.concurrent.Callable;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.Corrector;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBIBiosampleRunnable implements Callable<Void> {
    
    private final File inputfile;
    private final File outputfile;

    private static TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    
	private static Logger log = LoggerFactory.getLogger(NCBIBiosampleRunnable.class);

	public NCBIBiosampleRunnable(File inputfile, File outputfile) {
	    this.inputfile = inputfile;
	    this.outputfile = outputfile;
	}

	
	public static SampleData convert(File ncbiBiosampleXMLFile)
			throws DocumentException, ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException, FileNotFoundException {
		return convert(XMLUtils.getDocument(ncbiBiosampleXMLFile));
	}

	public static SampleData convert(Document document) throws ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {

		Element sampleSet = document.getRootElement();
		Element sample = XMLUtils.getChildByName(sampleSet, "BioSample");
		return convert(sample);
	}
	

	public static SampleData convert(Element sample) throws ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {

		SampleData st = new SampleData();

        Element description = XMLUtils.getChildByName(sample, "Description");
		
		String accession = sample.attributeValue("accession");
        String ncbiId = sample.attributeValue("id");

        st.msi.submissionIdentifier = "GNC-"+accession;
        
        Element ownerElem = XMLUtils.getChildByName(sample, "Owner");
        String ownerName = XMLUtils.getChildByName(ownerElem, "Name").getTextTrim();
        URL ownerURL = null;
        try {
            if (XMLUtils.getChildByName(ownerElem, "Name").attributeValue("url") != null 
                    && XMLUtils.getChildByName(ownerElem, "Name").attributeValue("url").length() > 0) {
                ownerURL = new URL(XMLUtils.getChildByName(ownerElem, "Name").attributeValue("url"));
            }
        } catch (MalformedURLException e) {
            log.warn("URL not well formed: "+XMLUtils.getChildByName(ownerElem, "Name").attributeValue("url"), e);
            ownerURL = null;
        }
        
        for (Element contactElem : XMLUtils.getChildrenByName(XMLUtils.getChildByName(ownerElem, "Contacts"), "Contact")) {
            String ownerEmail = contactElem.attributeValue("email");
            String ownerURLString = null;
            if (ownerURL != null) ownerURLString = ownerURL.toString();
            
            st.msi.organizations.add(new Organization(ownerName, null, ownerURLString, ownerEmail, "submitter"));
            
            Element name = XMLUtils.getChildByName(contactElem, "Name");
            if (name != null) {
                String firstName = null;
                String lastName = null;
                Element firstElem = XMLUtils.getChildByName(name, "First");
                if (firstElem != null) firstName = firstElem.getTextTrim();
                Element lastElem = XMLUtils.getChildByName(name, "Last");
                if (lastElem != null) lastName = lastElem.getTextTrim();
                
                st.msi.persons.add(new Person(lastName, "", firstName, ownerEmail, "submitter"));
            }
        }
        
        String sampleName = XMLUtils.getChildByName(description, "Title").getTextTrim();
        //if first character of name is a hash, Limpopo will interpret the line as a comment
        //to solve, remove the hash
        if (sampleName.startsWith("#")) {
            sampleName = sampleName.substring(1);
        }
        SampleNode sn = new SampleNode(sampleName);
        sn.setSampleAccession(accession);

        for (Element idElem : XMLUtils.getChildrenByName(XMLUtils.getChildByName(sample, "Ids"), "Id")) {
            String id = idElem.getTextTrim();
            if (!sn.getSampleAccession().equals(id)) {
                sn.addAttribute(new CommentAttribute("synonym", Corrector.cleanString(id)));
            }
        }
		
        Element descriptionCommment = XMLUtils.getChildByName(description, "Comment");
        if (descriptionCommment != null) {
            Element descriptionParagraph = XMLUtils.getChildByName(description, "Paragraph");
            if (descriptionParagraph != null) {
                String secondaryDescription = descriptionParagraph.getTextTrim();
                if (!sn.getNodeName().equals(secondaryDescription)) {
                    sn.addAttribute(new CommentAttribute("secondary description", Corrector.cleanString(secondaryDescription)));
                }
            }
        }
        
        //handle the organism
        Element organismElement = XMLUtils.getChildByName(description, "Organism");
        sn.addAttribute(new OrganismAttribute(Corrector.cleanString(organismElement.attributeValue("taxonomy_name")),
                st.msi.getOrAddTermSource(ncbitaxonomy),
                Integer.parseInt(organismElement.attributeValue("taxonomy_id"))));        
        
        //handle attributes
        for (Element attrElem : XMLUtils.getChildrenByName(XMLUtils.getChildByName(sample, "Attributes"), "Attribute")) {
            String type = attrElem.attributeValue("display_name");
            if (type == null || type.length() == 0) {
                type = attrElem.attributeValue("attribute_name");
            }
            String value = attrElem.getTextTrim();
            sn.addAttribute(new CharacteristicAttribute(Corrector.cleanString(type), Corrector.cleanString(value)));
        }

        //handle model and packages
        for (Element modelElem : XMLUtils.getChildrenByName(XMLUtils.getChildByName(sample, "Models"), "Model")) {
            sn.addAttribute(new CommentAttribute(Corrector.cleanString("model"), Corrector.cleanString(modelElem.getTextTrim())));
        }
        sn.addAttribute(new CommentAttribute(Corrector.cleanString("package"), Corrector.cleanString(XMLUtils.getChildByName(sample, "Package").getTextTrim())));
        
        //TODO handle links
        
		st.scd.addNode(sn);

		return st;
	}

    @Override
    public Void call() throws Exception {
        SampleData st = null;
        try {
            st = convert(inputfile);
        } catch (FileNotFoundException e) {
            log.error("Problem with "+inputfile, e);
            throw e;
        } catch (DocumentException e) {
            log.error("Problem with "+inputfile, e);
            throw e;
        } catch (ParseException e) {
            log.error("Problem with "+inputfile, e);
            throw e;
        } catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
            log.error("Problem with "+inputfile, e);
            throw e;
        }
        
        // write back out
        FileWriter out = null;
        try {
            out = new FileWriter(outputfile);
        } catch (IOException e) {
            log.error("Error opening " + outputfile, e);
            throw e;
        }

        Normalizer norm = new Normalizer();
        norm.normalize(st);

        SampleTabWriter sampletabwriter = new SampleTabWriter(out);
        try {
            sampletabwriter.write(st);
            sampletabwriter.close();
        } catch (IOException e) {
            log.error("Error writing " + outputfile, e);
            throw e;
        }
        
        return null;
    }

    
}
