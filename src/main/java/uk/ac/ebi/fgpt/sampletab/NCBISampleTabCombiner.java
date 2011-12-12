package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class NCBISampleTabCombiner {

	private Logger log = LoggerFactory.getLogger(getClass());

	private int maxident = 1000000; // max is 1,000,000
	private File rootdir = new File("ncbicopy");

	private static SAXReader reader = new SAXReader();

	private static ENAUtils enautils = ENAUtils.getInstance();

	// singleton instance
	private static final NCBIBiosampleToSampleTab converter = NCBIBiosampleToSampleTab
			.getInstance();;

	private NCBISampleTabCombiner() {
		// private constructor
	}

	private File getFileByIdent(int ident) {
		File subdir = new File(rootdir, "" + ((ident / 1000) * 1000));
		File xmlfile = new File(subdir, "" + ident + ".xml");
		return xmlfile;
	}

	public HashMap<String, HashSet<File>> getGroupings()
			throws DocumentException {

		HashMap<String, HashSet<File>> groupings = new HashMap<String, HashSet<File>>();

		for (int i = 0; i < maxident; i++) {
			File xmlfile = getFileByIdent(i);

			log.debug("Trying " + xmlfile);

			if (!xmlfile.exists()) {
				log.debug("Skipping " + xmlfile);
				continue;
			}

			Document xml = reader.read(xmlfile);

			Collection<String> groupids = new ArrayList<String>();
			Element root = xml.getRootElement();
			Element ids = XMLUtils.getChildByName(root, "Ids");
			Element attributes = XMLUtils.getChildByName(root, "Attributes");
			if (ids != null) {
				for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
					String dbname = id.attributeValue("db");
					String sampleid = id.getText();
					if (dbname.equals("SRA")) {
						// TODO group by sra study
						log.debug("Getting studies of SRA sample " + sampleid);
						Collection<String> studyids = enautils
								.getStudiesForSample(sampleid);
						if (studyids != null) {
							groupids.addAll(studyids);
						}
					} else if (dbname.equals("dbGaP")) {
						// group by dbGaP project
						if (attributes != null) {
							for (Element attribute : XMLUtils
									.getChildrenByName(attributes, "Attribute")) {
								if (attribute.attributeValue("attribute_name")
										.equals("gap_accession")) {
									groupids.add(attribute.getText());
								}
							}
						}
					} else if (dbname.equals("GSS")) {
						// TODO group by GSS project
						// GSS == Genome Survey Sequence
						if (sampleid.startsWith("LIBGSS_")) {
							Element owner = XMLUtils.getChildByName(root,
									"Owner");
							if (owner != null) {
								Element name = XMLUtils.getChildByName(owner,
										"Name");
								if (name != null) {
									String ownername = name.getText();
									groupids.add(ownername);
								}
							}
						}
					} else if (dbname.equals("EST")) {
						// TODO group by EST project
						// EST == Expressed Sequence Tag
						if (sampleid.startsWith("LIBEST_")) {
							Element owner = XMLUtils.getChildByName(root,
									"Owner");
							if (owner != null) {
								Element name = XMLUtils.getChildByName(owner,
										"Name");
								if (name != null) {
									String ownername = name.getText();
									groupids.add(ownername);
								}
							}
						}
					} else {
						// could group by others, but some of them are very big
					}
				}
			}
			for (String groupid : groupids) {
				HashSet<File> group;
				if (groupings.containsKey(groupid)) {
					group = groupings.get(groupid);
				} else {
					group = new HashSet<File>();
					groupings.put(groupid, group);
				}
				group.add(xmlfile);
			}
		}
		return groupings;

	}

	public void combine() {

		HashMap<String, HashSet<File>> groups;
		try {
			groups = getGroupings();
		} catch (DocumentException e1) {
			log.warn("Unable to group");
			e1.printStackTrace();
			return;
		}

		File outdir = new File("output");
		for (String group : groups.keySet()) {

			if (group == null || group.equals("")){
				log.info("Skipping empty group name");
				continue;
			}
			if (groups.get(group).size() < 1) {
				continue;
			}

			System.out.println(group + " : " + groups.get(group).size());

			File outsubdir = new File(outdir, group);
			outsubdir.mkdirs();

			SampleData sampleout = new SampleData();
			sampleout.msi.submissionIdentifier = "GNC-" + group;

			// log.info("Group : " + group);

			for (File xmlfile : groups.get(group)) {
				// log.debug("Group: " + group + " Filename: " + xmlfile);

				SampleData sampledata;
				try {
					sampledata = converter.convert(xmlfile);
				} catch (ParseException e2) {
					log.warn("Unable to convert " + xmlfile);
					e2.printStackTrace();
					continue;
				} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e2) {
					log.warn("Unable to convert " + xmlfile);
					e2.printStackTrace();
					continue;
				} catch (DocumentException e2) {
					log.warn("Unable to convert " + xmlfile);
					e2.printStackTrace();
					continue;
				}

				File outfile = new File(outsubdir, xmlfile.getName().replace(
						".xml", ".ncbi.sampletab.txt"));
				SampleTabWriter writer;

				try {
					writer = new SampleTabWriter(new FileWriter(outfile));
					writer.write(sampledata);
					writer.close();
				} catch (IOException e3) {
					log.warn("Unable to write " + outfile);
					e3.printStackTrace();
					continue;
				}

				// add nodes from here to parent
				for (SCDNode node : sampledata.scd.getRootNodes()) {
					try {
						sampleout.scd.addNode(node);
					} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e4) {
						log.warn("Unable to add node " + node.getNodeName());
						e4.printStackTrace();
						continue;
					}
				}

				for (int i = 0; i < sampledata.msi.organizationName.size(); i++) {
					if (!sampleout.msi.organizationName
							.contains(sampledata.msi.organizationName.get(i))) {
						sampleout.msi.organizationName
								.add(sampledata.msi.organizationName.get(i));
						sampleout.msi.organizationAddress
								.add(sampledata.msi.organizationAddress.get(i));
						sampleout.msi.organizationEmail
								.add(sampledata.msi.organizationEmail.get(i));
						sampleout.msi.organizationRole
								.add(sampledata.msi.organizationRole.get(i));
						sampleout.msi.organizationURI
								.add(sampledata.msi.organizationURI.get(i));
					}
				}

				for (int i = 0; i < sampledata.msi.personLastName.size(); i++) {
					// TODO this assumes no same surnamed people
					if (!sampleout.msi.personLastName
							.contains(sampledata.msi.personLastName.get(i))) {
						sampleout.msi.personLastName
								.add(sampledata.msi.personLastName.get(i));
						sampleout.msi.personInitials
								.add(sampledata.msi.personInitials.get(i));
						sampleout.msi.personFirstName
								.add(sampledata.msi.personFirstName.get(i));
						sampleout.msi.personEmail
								.add(sampledata.msi.personEmail.get(i));
						sampleout.msi.personRole.add(sampledata.msi.personRole
								.get(i));
					}
				}

				if (sampledata.msi.submissionReleaseDate != null) {
					if (sampleout.msi.submissionReleaseDate == null) {
						sampleout.msi.submissionReleaseDate = sampledata.msi.submissionReleaseDate;
					} else {
						// use the most recent of the two dates
						SimpleDateFormat dateFormatEBI = new SimpleDateFormat("yyyy/MM/dd");
						Date datadate = null;
						Date outdate = null;
						try {
							datadate = dateFormatEBI.parse(sampledata.msi.submissionReleaseDate);
							outdate = dateFormatEBI.parse(sampleout.msi.submissionReleaseDate);
						} catch (ParseException e) {
							log.error("unable to parse dates");
							e.printStackTrace();
						}
						if (datadate != null && outdate != null && datadate.after(outdate)){
							sampleout.msi.submissionReleaseDate = sampledata.msi.submissionReleaseDate;
							
						}
					}
				}
				if (sampledata.msi.submissionUpdateDate != null) {
					if (sampleout.msi.submissionUpdateDate == null) {
						sampleout.msi.submissionUpdateDate = sampledata.msi.submissionUpdateDate;
					} else {
						// use the most recent of the two dates
						SimpleDateFormat dateFormatEBI = new SimpleDateFormat("yyyy/MM/dd");
						Date datadate = null;
						Date outdate = null;
						try {
							datadate = dateFormatEBI.parse(sampledata.msi.submissionUpdateDate);
							outdate = dateFormatEBI.parse(sampleout.msi.submissionUpdateDate);
						} catch (ParseException e) {
							log.error("unable to parse dates");
							e.printStackTrace();
						}
						if (datadate != null && outdate != null && datadate.after(outdate)){
							sampleout.msi.submissionUpdateDate = sampledata.msi.submissionUpdateDate;
							
						}
					}
				}
			}

			if (sampleout.scd.getNodeCount() > 0) {
				// dont bother outputting if there are no samples...
				File outfile = new File(outsubdir, "sampletab.txt");
				SampleTabWriter writer;

				try {
					writer = new SampleTabWriter(new FileWriter(outfile));
					writer.write(sampleout);
					writer.close();
				} catch (IOException e) {
					log.warn("Unable to write " + outfile);
					e.printStackTrace();
					continue;
				}
			}
		}
	}

	public static void main(String[] args) {

		NCBISampleTabCombiner instance = new NCBISampleTabCombiner();
		instance.log.info("Starting combiner...");
		instance.combine();
	}

}
