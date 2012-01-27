package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.mged.magetab.error.ErrorCode;
import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.IDF;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.ExtractNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.LabeledExtractNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SDRFNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SourceNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.attribute.CharacteristicsAttribute;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.listener.ErrorItemListener;
import uk.ac.ebi.arrayexpress2.magetab.parser.IDFParser;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class MageTabToSampleTab {

	// singlton instance
	private static final MageTabToSampleTab instance = new MageTabToSampleTab();

	public static final MAGETABParser<MAGETABInvestigation> parser = new MAGETABParser<MAGETABInvestigation>();

	private SimpleDateFormat magetabdateformat = new SimpleDateFormat(
			"yyyy-MM-dd");

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());

	private MageTabToSampleTab() {
		// private constructor to prevent accidental multiple initialisations
	}

	public static MageTabToSampleTab getInstance() {
		return instance;
	}

	public Logger getLog() {
		return log;
	}

	public SampleData convert(String idfFilename) throws IOException, ParseException {
		return convert(new File(idfFilename));
	}

	public SampleData convert(File idfFile) throws IOException, ParseException {
        //a few idf files specify multiple sdrf files which may not all have been downloaded
        //due to a bug in limpopo, this can cause limpopo to hang indefinately.
        //therefore, first parse the idf only to see if this is something to avoid.
        
        IDFParser idfparser = new IDFParser();
        IDF idf = null;
        try {
            idf = idfparser.parse(idfFile);
        } catch (ParseException e) {
            System.err.println("Error parsing " + idfFile);
            e.printStackTrace();
            System.exit(1);
            return null;
        }
        if (idf.sdrfFile.size() != 1){
            System.err.println("Non-standard sdrf file references");
            System.err.println(idf.sdrfFile);
            System.exit(1);
            return null;
        }
        
		return convert(parser.parse(idfFile));
	}

	public SampleData convert(MAGETABInvestigation mt)
			throws ParseException {

		SampleData st = new SampleData();
		st.msi.submissionTitle = mt.IDF.investigationTitle;
		st.msi.submissionDescription = mt.IDF.experimentDescription;
		if (mt.IDF.publicReleaseDate != null && !mt.IDF.publicReleaseDate.trim().equals("")) {
			try{
			st.msi.submissionReleaseDate = magetabdateformat
					.parse(mt.IDF.publicReleaseDate.trim());
			} catch (java.text.ParseException e){
				log.error("Unable to parse release date "+mt.IDF.publicReleaseDate);
			}
		}
		// TODO update date
		st.msi.submissionIdentifier = "GA" + mt.IDF.accession;
		st.msi.submissionReferenceLayer = false;

		st.msi.publicationDOI = mt.IDF.publicationDOI;
		st.msi.publicationPubMedID = mt.IDF.pubMedId;

		st.msi.personLastName = mt.IDF.personLastName;
		st.msi.personInitials = mt.IDF.personMidInitials;
		st.msi.personFirstName = mt.IDF.personFirstName;
		st.msi.personEmail = mt.IDF.personEmail;
		// TODO fix minor spec mismatch when there are multiple roles for the
		// same person
		st.msi.personRole = mt.IDF.personRoles;

		// AE doesn't really have organisations, but does have affiliations
		// TODO check and remove duplicates
		st.msi.organizationName = mt.IDF.personAffiliation;
		st.msi.organizationAddress = mt.IDF.personAddress;
		// st.msi.organizationURI/Email/Role can't be mapped from ArrayExpress

		st.msi.databaseName.add("ArrayExpress");
		st.msi.databaseID.add(mt.IDF.accession);
		st.msi.databaseURI.add("http://www.ebi.ac.uk/arrayexpress/experiments/"
				+ mt.IDF.accession);

		// TODO check and remove duplicates
		st.msi.termSourceName = mt.IDF.termSourceName;
		st.msi.termSourceURI = mt.IDF.termSourceFile;
		st.msi.termSourceVersion = mt.IDF.termSourceVersion;

		// TODO add samples...
		// get the nodes that have relevant sample information
		// e.g. characteristics
		Collection<SDRFNode> samplenodes = new ArrayList<SDRFNode>();
		for (SDRFNode node : mt.SDRF.getNodes("sourcename")) {
			samplenodes.add(node);
		}
		for (SDRFNode node : mt.SDRF.getNodes("samplename")) {
			samplenodes.add(node);
		}
		for (SDRFNode node : mt.SDRF.getNodes("extractname")) {
			samplenodes.add(node);
		}
		for (SDRFNode node : mt.SDRF.getNodes("labeledextractname")) {
			samplenodes.add(node);
		}

		// now get nodes that are the topmost nodes
		ArrayList<SDRFNode> topnodes = new ArrayList<SDRFNode>();
		for (SDRFNode node : samplenodes) {
			if (node.getParentNodes().size() == 0) {
				topnodes.add(node);
			}
		}

		getLog().info("Creating node names");
		// create a sample from each topmost node
		for (SDRFNode sdrfnode : topnodes) {

			SampleNode scdnode = new SampleNode();
			String name = sdrfnode.getNodeName();
			getLog().info("processing " + name);
			scdnode.setNodeName(name);
			// since some attributes only exist for some sub-classes, need to
			// test
			// for instanceof for each of those sub-classes, cast accordingly
			// and then access the attributes
			List<CharacteristicsAttribute> characteristics = null;
			Map<String, String> comments = null;
			if (sdrfnode instanceof uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode) {
				// horribly long class references due to namespace collision
				uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode sdrfsamplenode = (uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode) sdrfnode;
				scdnode.sampleDescription = sdrfsamplenode.description;
				characteristics = sdrfsamplenode.characteristics;
				comments = sdrfsamplenode.comments;
			} else if (sdrfnode instanceof SourceNode) {
				SourceNode sdrfsourcenode = (SourceNode) sdrfnode;
				scdnode.sampleDescription = sdrfsourcenode.description;
				characteristics = sdrfsourcenode.characteristics;
				comments = sdrfsourcenode.comments;
			} else if (sdrfnode instanceof ExtractNode) {
				ExtractNode sdrfextractnode = (ExtractNode) sdrfnode;
				scdnode.sampleDescription = sdrfextractnode.description;
				characteristics = sdrfextractnode.characteristics;
				comments = sdrfextractnode.comments;
			} else if (sdrfnode instanceof LabeledExtractNode) {
				LabeledExtractNode sdrflabeledextractnode = (LabeledExtractNode) sdrfnode;
				scdnode.sampleDescription = sdrflabeledextractnode.description;
				characteristics = sdrflabeledextractnode.characteristics;
				comments = sdrflabeledextractnode.comments;
			}

			getLog().info("got characteristics");
			if (characteristics != null) {
				for (CharacteristicsAttribute sdrfcharacteristic : characteristics) {
					CharacteristicAttribute scdcharacteristic = new CharacteristicAttribute();
					scdcharacteristic.type = sdrfcharacteristic.type;
					scdcharacteristic.setAttributeValue(sdrfcharacteristic
							.getAttributeValue());
					if (sdrfcharacteristic.unit != null) {
						scdcharacteristic.unit = new UnitAttribute();
						scdcharacteristic.unit.termSourceREF = sdrfcharacteristic.unit.termSourceREF;
						scdcharacteristic.unit.termSourceID = sdrfcharacteristic.unit.termAccessionNumber;
					}
					scdcharacteristic.termSourceREF = sdrfcharacteristic.termSourceREF;
					scdcharacteristic.termSourceID = sdrfcharacteristic.termAccessionNumber;
					scdnode.addAttribute(scdcharacteristic);
				}
			}
			getLog().info("got comments");
			if (comments != null) {
				for (String key : comments.keySet()) {
					CommentAttribute comment = new CommentAttribute();
					comment.type = key;
					comment.setAttributeValue(comments.get(key));
					scdnode.addAttribute(comment);
				}
			}

			st.scd.addNode(scdnode);
		}

		getLog().info("Finished convert()");
		return st;
	}

	public void convert(MAGETABInvestigation mt, Writer writer)
			throws IOException, ParseException {
		getLog().debug("recieved magetab, preparing to convert");
		SampleData st = convert(mt);
		getLog().debug("sampletab converted, preparing to output");
		SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
		getLog().debug("created SampleTabWriter");
		sampletabwriter.write(st);
		sampletabwriter.close();

	}

	public void convert(File idfFile, Writer writer) throws IOException,
			ParseException {
        //a few idf files specify multiple sdrf files which may not all have been downloaded
        //due to a bug in limpopo, this can cause limpopo to hang indefinately.
        //therefore, first parse the idf only to see if this is something to avoid.

        log.info("Checking IDF");
        IDFParser idfparser = new IDFParser();
        IDF idf = null;
        try {
            idf = idfparser.parse(idfFile);
        } catch (ParseException e) {
            System.err.println("Error parsing " + idfFile);
            e.printStackTrace();
            System.exit(1);
            return;
        }
        log.info("Checking IDF");
        if (idf.sdrfFile.size() != 1){
            log.error("Non-standard sdrf file references");
            log.error(""+idf.sdrfFile);
            System.exit(1);
            return;
        }
        
		MAGETABInvestigation mt = parser.parse(idfFile);
		convert(mt, writer);
	}

	public void convert(File idffile, String stfilename) throws IOException,
			ParseException {
		convert(idffile, new File(stfilename));
	}

	public void convert(File idffile, File stfile) throws IOException,
			ParseException {
		convert(idffile, new FileWriter(stfile));
	}

	public void convert(String idffilename, Writer writer) throws IOException,
			ParseException {
		convert(new File(idffilename), writer);
	}

	public void convert(String idffilename, File stfile) throws IOException,
			ParseException, java.text.ParseException {
		convert(idffilename, new FileWriter(stfile));
	}

	public void convert(String idffilename, String stfilename)
			throws IOException, ParseException, java.text.ParseException {
		convert(idffilename, new File(stfilename));
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Must provide an MAGETAB IDF filename and a SampleTab output filename.");
			System.exit(1);
			return;
		}
		String idfFilename = args[0];
		String sampleTabFilename = args[1];

		try {
            getInstance().convert(idfFilename, sampleTabFilename);
        } catch (IOException e) {
            System.err.println("Error converting "+idfFilename);
            e.printStackTrace();
        } catch (ParseException e) {
            System.err.println("Error converting "+idfFilename);
            e.printStackTrace();
        } catch (java.text.ParseException e) {
            System.err.println("Error converting "+idfFilename);
            e.printStackTrace();
        }
	}
}
