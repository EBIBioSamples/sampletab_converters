package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBISampleTabCombiner {

	private Logger log = LoggerFactory.getLogger(getClass());

	private int maxident = 1000000; // max is 1,000,000
	private File rootdir = new File("ncbicopy");

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

	public Collection<String> getGroupIds(File xmlFile)
			throws DocumentException {

		log.debug("Trying " + xmlFile);

		SAXReader reader = new SAXReader();

		Document xml;
		xml = reader.read(xmlFile);

		Collection<String> groupids = new ArrayList<String>();
		Element root = xml.getRootElement();
		Element ids = XMLUtils.getChildByName(root, "Ids");
		Element attributes = XMLUtils.getChildByName(root, "Attributes");
		for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
			String dbname = id.attributeValue("db");
			String sampleid = id.getText();
			if (dbname.equals("SRA")) {
				// group by sra study
				log.debug("Getting studies of SRA sample " + sampleid);
				Collection<String> studyids = enautils
						.getStudiesForSample(sampleid);
				if (studyids != null) {
					groupids.addAll(studyids);
				}
			} else if (dbname.equals("dbGaP")) {
				// group by dbGaP project
				for (Element attribute : XMLUtils.getChildrenByName(attributes,
						"Attribute")) {
					if (attribute.attributeValue("attribute_name").equals(
							"gap_accession")) {
						groupids.add(attribute.getText());
					}
				}
			} else if (dbname.equals("EST") || dbname.equals("GSS")) {
				// EST == Expressed Sequence Tag
				// GSS == Genome Survey Sequence
				// group by owner
				
//				Element owner = XMLUtils.getChildByName(root, "Owner");
//				Element name = XMLUtils.getChildByName(owner, "Name");
//				if (name != null) {
//					String ownername = name.getText();
//					// clean ownername
//					ownername = ownername.toLowerCase();
//					ownername = ownername.trim();
//					String cleanname = "";
//					for (int j = 0; j < ownername.length(); j++) {
//						String c = ownername.substring(j, j + 1);
//						if (c.matches("[a-z0-9]")) {
//							cleanname += c;
//						}
//					}
//					groupids.add(cleanname);
//				}
				//this doesnt work so well by owner, so dont bother
				//may need to group samples from the same owner in a post-hoc manner?
				groupids.add(sampleid);
			} else {
				// could group by others, but some of them are very big
			}
		}
		return groupids;
	}

	public HashMap<String, Collection<File>> getGroupings()
			throws DocumentException {

		HashMap<String, Collection<File>> groupings = new HashMap<String, Collection<File>>();

		for (int i = 0; i < maxident; i = i + 1) {
			File xmlFile = getFileByIdent(i);

			if (!xmlFile.exists()) {
				log.debug("Skipping " + xmlFile);
				continue;
			}

			Collection<String> groupids = getGroupIds(xmlFile);

			
			for (String groupid : groupids) {
				Collection<File> group;
				if (groupings.containsKey(groupid)) {
					group = groupings.get(groupid);
				} else {
					group = new TreeSet<File>();
					groupings.put(groupid, group);
				}
				group.add(xmlFile);
			}
		}
		return groupings;

	}

	public void combine() {

		HashMap<String, Collection<File>> groups;
		try {
			groups = getGroupings();
		} catch (DocumentException e1) {
			log.warn("Unable to group");
			e1.printStackTrace();
			return;
		}

		File outdir = new File("output");
		for (String group : groups.keySet()) {

			if (group == null || group.equals("")) {
				log.info("Skipping empty group name");
				continue;
			}
			if (groups.get(group).size() < 1) {
				continue;
			}

			log.info(group + " : " + groups.get(group).size());

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

				// File outfile = new File(outsubdir, xmlfile.getName().replace(
				// ".xml", ".ncbi.sampletab.txt"));
				// SampleTabWriter writer;
				// try {
				// writer = new SampleTabWriter(new FileWriter(outfile));
				// writer.write(sampledata);
				// writer.close();
				// } catch (IOException e3) {
				// log.warn("Unable to write " + outfile);
				// e3.printStackTrace();
				// continue;
				// }

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

						if (i >= sampledata.msi.organizationAddress.size()) {
							sampleout.msi.organizationAddress.add("");
						} else {
							sampleout.msi.organizationAddress
									.add(sampledata.msi.organizationAddress
											.get(i));
						}

						if (i >= sampledata.msi.organizationEmail.size()) {
							sampleout.msi.organizationEmail.add("");
						} else {
							sampleout.msi.organizationEmail
									.add(sampledata.msi.organizationEmail
											.get(i));
						}

						if (i >= sampledata.msi.organizationRole.size()) {
							sampleout.msi.organizationRole.add("");
						} else {
							sampleout.msi.organizationRole
									.add(sampledata.msi.organizationRole.get(i));
						}

						if (i >= sampledata.msi.organizationURI.size()) {
							sampleout.msi.organizationURI.add("");
						} else {
							sampleout.msi.organizationURI
									.add(sampledata.msi.organizationURI.get(i));
						}
					}
				}

				for (int i = 0; i < sampledata.msi.personLastName.size(); i++) {
					// TODO this assumes no same surnamed people
					if (!sampleout.msi.personLastName
							.contains(sampledata.msi.personLastName.get(i))) {

						sampleout.msi.personLastName
								.add(sampledata.msi.personLastName.get(i));

						if (i >= sampledata.msi.personInitials.size()) {
							sampleout.msi.personInitials.add("");
						} else {
							sampleout.msi.personInitials
									.add(sampledata.msi.personInitials.get(i));
						}

						if (i >= sampledata.msi.personFirstName.size()) {
							sampleout.msi.personFirstName.add("");
						} else {
							sampleout.msi.personFirstName
									.add(sampledata.msi.personFirstName.get(i));
						}

						if (i >= sampledata.msi.personEmail.size()) {
							sampleout.msi.personEmail.add("");
						} else {
							sampleout.msi.personEmail
									.add(sampledata.msi.personEmail.get(i));
						}

						if (i >= sampledata.msi.personRole.size()) {
							sampleout.msi.personRole.add("");
						} else {
							sampleout.msi.personRole
									.add(sampledata.msi.personRole.get(i));
						}
					}
				}

				for (int i = 0; i < sampledata.msi.termSourceName.size(); i++) {
					if (!sampleout.msi.termSourceName
							.contains(sampledata.msi.termSourceName.get(i))) {

						sampleout.msi.termSourceName
								.add(sampledata.msi.termSourceName.get(i));

						if (i >= sampledata.msi.termSourceURI.size()) {
							sampleout.msi.termSourceURI.add("");
						} else {
							sampleout.msi.termSourceURI
									.add(sampledata.msi.termSourceURI.get(i));
						}

						if (i >= sampledata.msi.termSourceVersion.size()) {
							sampleout.msi.termSourceVersion.add("");
						} else {
							sampleout.msi.termSourceVersion
									.add(sampledata.msi.termSourceVersion
											.get(i));
						}
					}
				}

				sampleout.msi.databaseID.addAll(sampledata.msi.databaseID);
				sampleout.msi.databaseName.addAll(sampledata.msi.databaseName);
				sampleout.msi.databaseURI.addAll(sampledata.msi.databaseURI);

				sampleout.msi.publicationDOI
						.addAll(sampledata.msi.publicationDOI);
				sampleout.msi.publicationPubMedID
						.addAll(sampledata.msi.publicationPubMedID);

				if (sampledata.msi.submissionReleaseDate != null) {
					if (sampleout.msi.submissionReleaseDate == null
							|| sampleout.msi.submissionReleaseDate.equals("")) {
						sampleout.msi.submissionReleaseDate = sampledata.msi.submissionReleaseDate;
					} else {
						// use the most recent of the two dates
						SimpleDateFormat dateFormatEBI = new SimpleDateFormat(
								"yyyy/MM/dd");
						Date datadate = null;
						Date outdate = null;
						try {
							datadate = dateFormatEBI
									.parse(sampledata.msi.submissionReleaseDate);
							outdate = dateFormatEBI
									.parse(sampleout.msi.submissionReleaseDate);
						} catch (ParseException e) {
							log.error("unable to parse dates");
							e.printStackTrace();
						}
						if (datadate != null && outdate != null
								&& datadate.after(outdate)) {
							sampleout.msi.submissionReleaseDate = sampledata.msi.submissionReleaseDate;

						}
					}
				}
				if (sampledata.msi.submissionUpdateDate != null) {
					if (sampleout.msi.submissionUpdateDate == null
							|| sampleout.msi.submissionUpdateDate.equals("")) {
						sampleout.msi.submissionUpdateDate = sampledata.msi.submissionUpdateDate;
					} else {
						// use the most recent of the two dates
						SimpleDateFormat dateFormatEBI = new SimpleDateFormat(
								"yyyy/MM/dd");
						Date datadate = null;
						Date outdate = null;
						try {
							datadate = dateFormatEBI
									.parse(sampledata.msi.submissionUpdateDate);
							outdate = dateFormatEBI
									.parse(sampleout.msi.submissionUpdateDate);
						} catch (ParseException e) {
							log.error("unable to parse dates");
							e.printStackTrace();
						}
						if (datadate != null && outdate != null
								&& datadate.after(outdate)) {
							sampleout.msi.submissionUpdateDate = sampledata.msi.submissionUpdateDate;

						}
					}
				}
			}

			// sanity checks to make sure sensible things happened
			if (sampleout.scd.getRootNodes().size() != groups.get(group).size()) {
				log.warn("unequal sizes: "
						+ sampleout.scd.getRootNodes().size() + " vs "
						+ groups.get(group).size());
			}

			if (sampleout.scd.getNodeCount() > 0) {
				// dont bother outputting if there are no samples...
				File outfile = new File(outsubdir, "sampletab.txt");
				SampleTabWriter writer;

				try {
					writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outfile)));
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
