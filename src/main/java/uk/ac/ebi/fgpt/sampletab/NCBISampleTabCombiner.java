package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
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

	private int maxident = 1000; // max is 1,000,000
	private File rootdir = new File("ncbicopy");

	private static SAXReader reader = new SAXReader();

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

			Element root = xml.getRootElement();
			Element ids = XMLUtils.getChildByName(root, "Ids");
			if (ids != null) {
				for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
					String dbname = id.attributeValue("db");
					Collection<String> groupids = new ArrayList<String>();
					if (dbname.equals("SRA")) {
						String sampleid = id.getText();
						// TODO group by sra study
						log.info("Getting studies of SRA sample " + sampleid);
						Collection<String> studyids = ENAUtils.getInstance()
								.getStudiesForSample(sampleid);
						if (studyids != null) {
							groupids.addAll(studyids);
						}
					} else if (dbname.equals("dbGaP")) {
						// TODO group by dbGaP project
						// Characteristic[study name]
					} else if (dbname.equals("GSS")) {
						// TODO group by GSS project
						// GSS == Genome Survey Sequence
					} else if (dbname.equals("EST")) {
						// TODO group by EST project
						// EST == Expressed Sequence Tag
					} else {
						// could group by others, but some of them are very big
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

			if (groups.get(group).size() < 5) {
				continue;
			}

			File outsubdir = new File(outdir, group);
			outsubdir.mkdirs();

			SampleData sampleout = new SampleData();
			sampleout.msi.submissionIdentifier = "GNC-" + group;

			log.info("Group : " + group);

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
			}

			if (sampleout.scd.getNodeCount() > 0) {
				// dont bother outputting if there are no samples...
				File outfile = new File(outsubdir,"sampletab.txt");
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
