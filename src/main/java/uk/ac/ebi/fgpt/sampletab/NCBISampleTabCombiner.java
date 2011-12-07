package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class NCBISampleTabCombiner {

	private Logger log = LoggerFactory.getLogger(getClass());

	private int maxident = 1000000; // max is 1,000,000
	private File rootdir = new File("ncbicopy");

	private final DocumentBuilderFactory builderFactory = DocumentBuilderFactory
			.newInstance();
	private DocumentBuilder builder;

	private NCBIBiosampleToSampleTab converter;

	private NCBISampleTabCombiner() {
		// private constructor

		converter = NCBIBiosampleToSampleTab.getInstance();

		try {
			builder = builderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			log.error("Unable to create new DocumentBuilder");
			e.printStackTrace();
			return;
		}
	}

	private File getFileByIdent(int ident) {
		File subdir = new File(rootdir, "" + ((ident / 1000) * 1000));
		File xmlfile = new File(subdir, "" + ident + ".xml");
		return xmlfile;
	}

	public HashMap<String, HashSet<File>> getGroupings() {

		HashMap<String, HashSet<File>> groupings = new HashMap<String, HashSet<File>>();

		for (int i = 0; i < maxident; i++) {
			File xmlfile = getFileByIdent(i);

			log.debug("Trying " + xmlfile);

			if (!xmlfile.exists()) {
				log.debug("Skipping " + xmlfile);
				continue;
			}

			Document xml;
			try {
				xml = builder.parse(xmlfile);
			} catch (SAXException e) {
				log.warn("Unable to parse " + xmlfile);
				e.printStackTrace();
				continue;
			} catch (IOException e) {
				log.warn("Unable to parse " + xmlfile);
				e.printStackTrace();
				continue;
			}

			Element root = xml.getDocumentElement();
			Element ids = XMLUtils.getChildByName(root, "Ids");
			if (ids != null) {
				for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
					String dbname = id.getAttribute("db");
					String groupid = null;
					if (dbname.equals("SRA")) {
						String sampleid = id.getTextContent();
						// TODO group by sra study
						try {
							groupid = ENAUtils.getInstance().getStudyForSample(
									sampleid);
						} catch (DOMException e) {
							log.warn("Unable to get study of " + sampleid);
							e.printStackTrace();
							continue;
						} catch (SAXException e) {
							log.warn("Unable to get study of " + sampleid);
							e.printStackTrace();
							continue;
						} catch (IOException e) {
							log.warn("Unable to get study of " + sampleid);
							e.printStackTrace();
							continue;
						} catch (ParserConfigurationException e) {
							log.warn("Unable to get study of " + sampleid);
							e.printStackTrace();
							continue;
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
					if (groupid != null) {
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
		HashMap<String, HashSet<File>> groups = getGroupings();

		File outdir = new File("output");
		for (String group : groups.keySet()) {
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
				} catch (SAXException e) {
					log.warn("Unable to convert " + xmlfile);
					e.printStackTrace();
					continue;
				} catch (IOException e) {
					log.warn("Unable to convert " + xmlfile);
					e.printStackTrace();
					continue;
				} catch (ParseException e) {
					log.warn("Unable to convert " + xmlfile);
					e.printStackTrace();
					continue;
				} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
					log.warn("Unable to convert " + xmlfile);
					e.printStackTrace();
					continue;
				}

				File outfile = new File(outsubdir, xmlfile.getName().replace(
						".xml", ".ncbi.sampletab.txt"));
				SampleTabWriter writer;

				try {
					writer = new SampleTabWriter(new FileWriter(outfile));
					writer.write(sampledata);
					writer.close();
				} catch (IOException e) {
					log.warn("Unable to write " + outfile);
					e.printStackTrace();
					continue;
				}

				// add nodes from here to parent
				for (SCDNode node : sampledata.scd.getRootNodes()) {
					try {
						sampleout.scd.addNode(node);
					} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
						log.warn("Unable to add node " + node.getNodeName());
						e.printStackTrace();
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
				File outfile = new File(outdir, group + ".sampletab.txt");
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
