package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBIBiosampleRunnable implements Runnable {
    
    private final File inputfile;
    private final File outputfile;
    
	private Logger log = LoggerFactory.getLogger(getClass());

	public NCBIBiosampleRunnable(File inputfile, File outputfile) {
	    this.inputfile = inputfile;
	    this.outputfile = outputfile;
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
				toreturn = toreturn + street.getTextTrim() + ", ";
			}
			if (city != null) {
				toreturn = toreturn + city.getTextTrim() + ", ";
			}
			if (sub != null) {
				toreturn = toreturn + sub.getTextTrim() + ", ";
			}
			if (country != null) {
				toreturn = toreturn + country.getTextTrim();
			}
		}
		return toreturn;
	}
	
	public SampleData convert(File ncbiBiosampleXMLFile)
			throws DocumentException, ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException, FileNotFoundException {
		return convert(XMLUtils.getDocument(ncbiBiosampleXMLFile));
	}

	public SampleData convert(Document ncbiBiosampleXML) throws ParseException,
			uk.ac.ebi.arrayexpress2.magetab.exception.ParseException {

		SampleData st = new SampleData();
		Element root = ncbiBiosampleXML.getRootElement();
		Element description = XMLUtils.getChildByName(root, "Description");
		Element title = XMLUtils.getChildByName(description, "Title");
		Element descriptioncomment = XMLUtils.getChildByName(description,
				"Comment");
		Element descriptionparagraph = null;
		Element descriptiontable = null;
		if (descriptioncomment != null) {
			descriptionparagraph = XMLUtils.getChildByName(descriptioncomment,
					"Paragraph");
			descriptiontable = XMLUtils.getChildByName(descriptioncomment,
					"Table");
		}
		Element owner = XMLUtils.getChildByName(root, "Owner");
		Element contacts = XMLUtils.getChildByName(owner, "Contacts");
		Element links = XMLUtils.getChildByName(owner, "Links"); 
		Element ids = XMLUtils.getChildByName(root, "Ids");
		Element attributes = XMLUtils.getChildByName(root, "Attributes");
		Element organism = XMLUtils.getChildByName(description, "Organism");

		// TODO unencode http conversion, e.g. &amp, if this is an issue
		st.msi.submissionTitle = title.getTextTrim();

		SimpleDateFormat dateFormatNCBI = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss");
		// some NCBI data has milliseconds, some doesn't.
		// therefore need two formatters. try one, on fail try the other
		SimpleDateFormat dateFormatNCBImilisecond = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date publicationDate;
		try {
			publicationDate = dateFormatNCBI.parse(root
					.attributeValue("publication_date"));
		} catch (ParseException e) {
			publicationDate = dateFormatNCBImilisecond.parse(root
					.attributeValue("publication_date"));
		}

		st.msi.submissionReleaseDate = publicationDate;
		// NCBI Biosamples does not always have a last_update attribute
		if (root.attributeValue("last_update") != null
				&& root.attributeValue("last_update").equals("")) {
			st.msi.submissionUpdateDate = st.msi.submissionReleaseDate;
		} else if (root.attributeValue("last_update") != null) {
			Date updateDate = dateFormatNCBI.parse(root
					.attributeValue("last_update"));
			st.msi.submissionUpdateDate = updateDate;
		}

		// NCBI Biosamples identifier numbers are prefixed by GNC to get the
		// submission identifier
		// Note that NCBI uses 1 sample = 1 submission, but EBI allows many
		// samples in one submission
		st.msi.submissionIdentifier = "GNC-" + root.attributeValue("id");
		if (descriptionparagraph != null) {
			if (!descriptionparagraph.getTextTrim().equals("none provided")) {
				st.msi.submissionDescription = descriptionparagraph.getTextTrim();
			}
		}

		String organizationName = owner.getTextTrim();

		if (contacts != null) {
			// log.info("Processing contacts");
			for (Element contact : XMLUtils.getChildrenByName(contacts,
					"Contact")) {
				Element name = XMLUtils.getChildByName(contact, "Name");
				Element address = XMLUtils.getChildByName(contact, "Address");
				if (name != null) {
					// this has a name, therefore it is a person
					Element last = XMLUtils.getChildByName(name, "Last");
					Element first = XMLUtils.getChildByName(name, "First");
					Element middle = XMLUtils.getChildByName(name, "Middle");
					
					
					String lastname = last.getTextTrim();
					String initials = null;
                    // TODO fix middlename == initials assumption
                    if (middle != null) {
                        initials = middle.getTextTrim();
                    }
					String firstname = null;
                    if (first != null) {
                        firstname = first.getTextTrim();
                    }
					String email = contact.attributeValue("email");
					String role = null;
					Person per = new Person(lastname, initials, firstname, email, role);
					if (!st.msi.persons.contains(per)){
					    st.msi.persons.add(per);
					}
				} else {
					// no name of this contact, therefore it is an
					// Organisation

                    // NCBI doesn't have roles or URIs
                    // Also, NCBI only allows one organisation per
                    // sample
					Organization org = new Organization(organizationName, addressToString(address), null, contact
                            .attributeValue("email"), null);

                    if (!st.msi.organizations.contains(org)){
                        st.msi.organizations.add(org);
                    }
				}
			}
		}
		if (links != null) {
			for (Element link : XMLUtils.getChildrenByName(links, "Links")) {
				// pubmedids
				// could be url, db_xref or entrez
				// entrez never seen to date, deprecated?
				if (link.attributeValue("type") == "db_xref"
						&& link.attributeValue("target") == "pubmed") {
					String PubMedID = link.getTextTrim();
					st.msi.publications.add(new Publication(PubMedID, null));
				}
			}
		}
		
		st.msi.termSources.add(new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null));

		SampleNode scdnode = new SampleNode();
		//name nodes by identifier to guarantee they won't overwrite when combined.
		scdnode.setNodeName("SAMN" + root.attributeValue("id"));
		scdnode.setSampleDescription(title.getTextTrim());
		scdnode.setSampleAccession("SAMN" + root.attributeValue("id"));
		OrganismAttribute organismAttrib = new OrganismAttribute();
		organismAttrib.setAttributeValue(organism
				.attributeValue("taxonomy_name"));
		organismAttrib.setTermSourceREF("NCBI Taxonomy");
		organismAttrib.setTermSourceID(organism.attributeValue("taxonomy_id"));
		scdnode.addAttribute(organismAttrib);

		for (Element row : XMLUtils.getChildrenByName(
				XMLUtils.getChildByName(descriptiontable, "Body"), "Row")) {
			// convert to an array list to ensure random access.
			ArrayList<Element> cells = new ArrayList<Element>(
					XMLUtils.getChildrenByName(row, "Cell"));

			CommentAttribute attrib = new CommentAttribute();
			attrib.setAttributeValue(cells.get(1).getTextTrim());
			attrib.type = cells.get(0).getTextTrim();
			scdnode.addAttribute(attrib);
		}

		for (Element attribute : XMLUtils.getChildrenByName(attributes,
				"Attribute")) {
			CommentAttribute attrib = new CommentAttribute();
			attrib.setAttributeValue(attribute.getTextTrim());
			attrib.type = attribute.attributeValue("attribute_name");
			// Dictionary name is more like a template than an ontology
			//  do not use 
//			if (attribute.attributeValue("dictionary_name") != null) {
//				attrib.setTermSourceREF(attribute
//						.attributeValue("dictionary_name"));
//			}
			scdnode.addAttribute(attrib);
		}

		DatabaseAttribute databaseAttrib = new DatabaseAttribute();
		databaseAttrib.setAttributeValue("NCBI Biosamples");
		databaseAttrib.databaseID = root.attributeValue("id");
		databaseAttrib.databaseURI = "http://www.ncbi.nlm.nih.gov/biosample?term="
				+ root.attributeValue("id") + "%5Buid%5D";
		scdnode.addAttribute(databaseAttrib);
		if (ids != null) {
			for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
				log.debug("Found an id");
				// a child element to process
				databaseAttrib = new DatabaseAttribute();
				String dbname = id.attributeValue("db");
				databaseAttrib.setAttributeValue(dbname);
				databaseAttrib.databaseID = id.getTextTrim();
				// databaseURI has different construction rules for different
				// databases
				// TODO clear up potential URL encoding problems
				if (dbname == null){
				    //do nothing more
				} else if (dbname.equals("SRA")) {
					databaseAttrib.databaseURI = "http://www.ebi.ac.uk/ena/data/view/"
							+ id.getTextTrim();
				} else if (dbname.equals("Coriell")) {
					if (!id.getTextTrim().equals("N/A")) {
						databaseAttrib.databaseURI = "http://ccr.coriell.org/Sections/Search/Sample_Detail.aspx?Ref="
								+ id.getTextTrim();
					}
				} else if (dbname.equals("HapMap")) {
					// TODO work out how to do this,
					// http://ccr.coriell.org/Sections/Collections/NHGRI/?SsId=11
					// is a starting point
				} else if (dbname.equals("EST")) {
					// One sample corresponds to many many ESTs generated from
					// that sample 
					// Can Search by the LIBEST_xxxxxxxx identifier in free text to
					// find them but no way to encode this search in the URL
				} else if (dbname.equals("GSS")) {
					// One sample corresponds to many many ESTs generated from
					// that sample 
					// Can Search by the LIBGSS_xxxxxxxx identifier in free text to
					// find them but no way to encode this search in the URL
				} else if (dbname.equals("BioSample")) {
				    //already have a biosamples identifier, so ignore this one.
				}
				scdnode.addAttribute(databaseAttrib);
			}
		}

		st.scd.addNode(scdnode);

		return st;
	}

    public void run() {
        SampleData st = null;
        try {
            st = convert(inputfile);
        } catch (FileNotFoundException e) {
            log.error("Problem with "+inputfile, e);
            return;
        } catch (DocumentException e) {
            log.error("Problem with "+inputfile, e);
            return;
        } catch (ParseException e) {
            log.error("Problem with "+inputfile, e);
            return;
        } catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
            log.error("Problem with "+inputfile, e);
            return;
        }
        
        // write back out
        FileWriter out = null;
        try {
            out = new FileWriter(outputfile);
        } catch (IOException e) {
            log.error("Error opening " + outputfile, e);
            return;
        }

        Normalizer norm = new Normalizer();
        norm.normalize(st);

        SampleTabWriter sampletabwriter = new SampleTabWriter(out);
        try {
            sampletabwriter.write(st);
            sampletabwriter.close();
        } catch (IOException e) {
            log.error("Error writing " + outputfile, e);
            return;
        }
        
    }

    
}
