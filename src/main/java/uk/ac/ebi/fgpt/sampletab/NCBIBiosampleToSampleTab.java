package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class NCBIBiosampleToSampleTab {

	// singlton instance
	private static final NCBIBiosampleToSampleTab instance = new NCBIBiosampleToSampleTab();
	// logging
	private Logger log = LoggerFactory.getLogger(getClass());
	// parsing
	private DocumentBuilderFactory builderFactory = DocumentBuilderFactory
			.newInstance();
	private DocumentBuilder builder;

	private NCBIBiosampleToSampleTab() {
		// private constructor to prevent accidental multiple initialisations
		try {
			builder = builderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			log.error("Unable to create new DocumentBuilder");
			e.printStackTrace();
		}
	}

	public static NCBIBiosampleToSampleTab getInstance() {
		return instance;
	}

	public String addressToString(Element address) {
		String toreturn = "";
		if (address != null) {
			Element street = XMLUtils.getChildByName(address, "Street");
			Element city = XMLUtils.getChildByName(address, "City");
			Element sub = XMLUtils.getChildByName(address, "Sub");
			Element country = XMLUtils.getChildByName(address, "Country");
			// TODO handle joins better
			if (street != null) {
				toreturn = toreturn + street.getTextContent() + ", ";
			}
			if (city != null) {
				toreturn = toreturn + city.getTextContent() + ", ";
			}
			if (sub != null) {
				toreturn = toreturn + sub.getTextContent() + ", ";
			}
			if (country != null) {
				toreturn = toreturn + country.getTextContent();
			}
		}
		return toreturn;
	}

	public SampleData convert(URL ncbiBiosampleXMLURL) throws SAXException,
			IOException, ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {
		return convert(builder.parse(ncbiBiosampleXMLURL.openStream()));
	}

	public SampleData convert(String ncbiBiosampleXMLFilename)
			throws SAXException, IOException, ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {
		return convert(new File(ncbiBiosampleXMLFilename));
	}

	public SampleData convert(File ncbiBiosampleXMLFile) throws SAXException,
			IOException, ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {
		return convert(builder.parse(ncbiBiosampleXMLFile));
	}

	public SampleData convert(Document ncbiBiosampleXML) throws ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {

		SampleData st = new SampleData();
		Element root = ncbiBiosampleXML.getDocumentElement();
		Element description = XMLUtils.getChildByName(root, "Description");
		Element title = XMLUtils.getChildByName(description, "Title");
		Element descriptioncomment = XMLUtils.getChildByName(description,
				"Comment");
		Element descriptionparagraph = null;
		if (descriptioncomment != null) {
			descriptionparagraph = XMLUtils.getChildByName(descriptioncomment,
					"Paragraph");
		}
		Element owner = XMLUtils.getChildByName(root, "Owner");
		Element contacts = XMLUtils.getChildByName(owner, "Contacts");
		Element links = XMLUtils.getChildByName(owner, "Links");
		Element ids = XMLUtils.getChildByName(root, "Ids");
		Element attributes = XMLUtils.getChildByName(root, "Attributes");
		Element organism = XMLUtils.getChildByName(description, "Organism");

		// TODO unencode http conversion, e.g. &amp, if this is an issue
		st.msi.submissionTitle = title.getTextContent();

		SimpleDateFormat dateFormatEBI = new SimpleDateFormat("yyyy/MM/dd");
		SimpleDateFormat dateFormatNCBI = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss");
		// some NCBI data has milliseconds, some doesn't.
		// therefore need two formatters. try one, on fail try the other
		SimpleDateFormat dateFormatNCBImilisecond = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date publicationDate;
		try {
			publicationDate = dateFormatNCBI.parse(root
					.getAttribute("publication_date"));
		} catch (ParseException e) {
			publicationDate = dateFormatNCBImilisecond.parse(root
					.getAttribute("publication_date"));
		}

		st.msi.submissionReleaseDate = dateFormatEBI.format(publicationDate);
		Date updateDate = dateFormatNCBI
				.parse(root.getAttribute("last_update"));
		st.msi.submissionUpdateDate = dateFormatEBI.format(updateDate);

		// NCBI Biosamples identifier numbers are prefixed by GNC to get the
		// submission identifier
		// Note that NCBI uses 1 sample = 1 submission, but EBI allows many
		// samples in one submission
		st.msi.submissionIdentifier = "GNC-" + root.getAttribute("id");
		if (descriptionparagraph != null) {
			if (!descriptionparagraph.getTextContent().equals("none provided")) {
				st.msi.submissionDescription = descriptionparagraph
						.getTextContent();
			}
		}

		String organizationName = owner.getTextContent();

		if (contacts != null) {
			for (Element contact : XMLUtils.getChildrenByName(contacts,
					"Contact")) {
				Element name = XMLUtils.getChildByName(contact, "Name");
				Element address = XMLUtils.getChildByName(contact, "Address");
				if (name != null) {
					// this has a name, therefore it is a person
					Element last = XMLUtils.getChildByName(name, "Last");
					Element first = XMLUtils.getChildByName(name, "First");
					Element middle = XMLUtils.getChildByName(name, "Middle");
					st.msi.personLastName.add(last.getTextContent());
					if (first != null) {
						st.msi.personFirstName.add(first.getTextContent());
					}
					// TODO fix middlename == initials assumption
					if (middle != null) {
						st.msi.personInitials.add(middle.getTextContent());
					}
					st.msi.personEmail.add(contact.getAttribute("email"));
				} else {
					// no name of this contact, therefore it is an
					// Organisation
					st.msi.organizationName.add(organizationName);
					st.msi.organizationAddress.add(addressToString(address));
					st.msi.organizationEmail.add(contact.getAttribute("email"));
					// NCBI doesn't have roles or URIs
					// Also, NCBI only allows one organisation per
					// sample
					st.msi.organizationRole.add("");
					st.msi.organizationURI.add("");
				}
			}
		}
		if (links != null) {
			for (Element link : XMLUtils.getChildrenByName(links, "Links")) {
				// pubmedids
				// could be url, db_xref or entrez
				// entrez never seen to date, deprecated?
				if (link.getAttribute("type") == "db_xref"
						&& link.getAttribute("target") == "pubmed") {
					String PubMedID = link.getTextContent();
					st.msi.publicationPubMedID.add(PubMedID);
					st.msi.publicationDOI.add("");
				}
			}
		}

		st.msi.termSourceName.add("NEWT");
		st.msi.termSourceURI.add("http://www.uniprot.org/taxonomy/");
		st.msi.termSourceVersion.add("");

		SampleNode scdnode = new SampleNode();
		scdnode.setNodeName(st.msi.submissionTitle);
		scdnode.sampleDescription = st.msi.submissionDescription;
		scdnode.sampleAccession = "SAMN" + root.getAttribute("id");
		OrganismAttribute organismAttrib = new OrganismAttribute();
		organismAttrib
				.setAttributeValue(organism.getAttribute("taxonomy_name"));
		organismAttrib.setTermSourceREF("NEWT");
		organismAttrib.setTermSourceID(organism.getAttribute("taxonomy_id"));
		scdnode.addAttribute(organismAttrib);

		if (attributes != null) {
			for (Element attribute : XMLUtils.getChildrenByName(attributes,
					"Attribute")) {
				CharacteristicAttribute attrib = new CharacteristicAttribute();
				attrib.setAttributeValue(attribute.getTextContent());
				attrib.type = attribute.getAttribute("attribute_name");
				// Dictionary name is kind of like ontology, but not.
				if (attribute.getAttribute("dictionary_name") != null) {
					attrib.termSourceREF = attribute
							.getAttribute("dictionary_name");
				}
				scdnode.addAttribute(attrib);
			}
		}

		DatabaseAttribute databaseAttrib = new DatabaseAttribute();
		databaseAttrib.setAttributeValue("NCBI Biosamples");
		databaseAttrib.databaseID = root.getAttribute("id");
		databaseAttrib.databaseURI = "http://www.ncbi.nlm.nih.gov/biosample?term="
				+ root.getAttribute("id") + "%5Buid%5D";
		scdnode.addAttribute(databaseAttrib);
		if (ids != null) {
			for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
				log.debug("Found an id");
				// a child element to process
				databaseAttrib = new DatabaseAttribute();
				String dbname = id.getAttribute("db");
				databaseAttrib.setAttributeValue(dbname);
				databaseAttrib.databaseID = id.getTextContent();
				// databaseURI has different construction rules for different
				// databases
				// TODO clear up potential URL encoding problems
				if (dbname.equals("SRA")) {
					databaseAttrib.databaseURI = "http://www.ebi.ac.uk/ena/data/view/"
							+ id.getTextContent();
				} else if (dbname.equals("Coriell")) {
					if (!id.getTextContent().equals("N/A")) {
						databaseAttrib.databaseURI = "http://ccr.coriell.org/Sections/Search/Sample_Detail.aspx?Ref="
								+ id.getTextContent();
					}
				} else if (dbname.equals("HapMap")) {
					// TODO work out how to do this,
					// http://ccr.coriell.org/Sections/Collections/NHGRI/?SsId=11
					// is a starting point
				}
				scdnode.addAttribute(databaseAttrib);
			}
		}

		st.scd.addNode(scdnode);

		return st;
	}

	public static void main(String[] args) {
		String ncbiBiosampleXMLFilename = args[0];
		String sampleTabFilename = args[1];

		NCBIBiosampleToSampleTab converter = NCBIBiosampleToSampleTab
				.getInstance();

		SampleData st = null;
		try {
			st = converter.convert(ncbiBiosampleXMLFilename);
		} catch (SAXException e) {
			System.out.println("Error converting " + ncbiBiosampleXMLFilename);
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error converting " + ncbiBiosampleXMLFilename);
			e.printStackTrace();
		} catch (ParseException e) {
			System.out.println("Error converting " + ncbiBiosampleXMLFilename);
			e.printStackTrace();
		} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
			System.out.println("Error converting " + ncbiBiosampleXMLFilename);
			e.printStackTrace();
		}

		FileWriter out = null;
		try {
			out = new FileWriter(sampleTabFilename);
		} catch (IOException e) {
			System.out.println("Error opening " + sampleTabFilename);
			e.printStackTrace();
		}

		SampleTabWriter sampletabwriter = new SampleTabWriter(out);
		try {
			sampletabwriter.write(st);
		} catch (IOException e) {
			System.out.println("Error writing " + sampleTabFilename);
			e.printStackTrace();
		}
	}
}
