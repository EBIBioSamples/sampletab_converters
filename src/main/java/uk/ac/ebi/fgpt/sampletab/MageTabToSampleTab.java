package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.ExtractNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.LabeledExtractNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SDRFNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SourceNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.attribute.CharacteristicsAttribute;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;

public class MageTabToSampleTab {
	
	private static final MageTabToSampleTab instance = new MageTabToSampleTab();
	public static final MAGETABParser<MAGETABInvestigation> parser = new MAGETABParser<MAGETABInvestigation>();
	
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

	private MageTabToSampleTab(){
		//private constructor to prevent accidental multiple initialisations
	}
	 
    public static MageTabToSampleTab getInstance() {
            return instance;
    }

    public Logger getLog() {
        return log;
    }
	
	public SampleData convert(String idfFilename) throws IOException,
			ParseException {
		return convert(new File(idfFilename));
	}

	public SampleData convert(File idfFile) throws IOException,
			ParseException {
		return convert(parser.parse(idfFile));
	}

	public SampleData convert(URL idfURL) throws IOException,
			ParseException {
		return convert(parser.parse(idfURL));
	}

	public SampleData convert(InputStream dataIn) throws ParseException {
		return convert(parser.parse(dataIn));
	}
		
	public SampleData convert(MAGETABInvestigation mt) throws ParseException{
		
		SampleData st = new SampleData();
		st.msi.submissionTitle = mt.IDF.investigationTitle;
		st.msi.submissionDescription = mt.IDF.experimentDescription;
		st.msi.submissionReleaseDate = mt.IDF.publicReleaseDate;
		st.msi.submissionIdentifier = "GA"+mt.IDF.accession;
		st.msi.submissionReferenceLayer = "false";
		
		st.msi.publicationDOI = mt.IDF.publicationDOI;
		st.msi.publicationPubMedID = mt.IDF.pubMedId;
		
		st.msi.personLastName = mt.IDF.personLastName;
		st.msi.personInitials = mt.IDF.personMidInitials;
		st.msi.personFirstName = mt.IDF.personFirstName;
		st.msi.personEmail = mt.IDF.personEmail;
		//TODO fix minor spec mismatch when there are multiple roles for the same person
		st.msi.personRole = mt.IDF.personRoles;
		
		//AE doesn't really have organisations, but does have affiliations
		//TODO check and remove duplicates
		st.msi.organizationName = mt.IDF.personAffiliation;
		st.msi.organizationAddress = mt.IDF.personAddress;
		//st.msi.organizationURI/Email/Role can't be mapped from ArrayExpress
		
		st.msi.databaseName.add("ArrayExpress");
		st.msi.databaseID.add(mt.IDF.accession);
		st.msi.databaseURI.add("http://www.ebi.ac.uk/arrayexpress/experiments/"+mt.IDF.accession);
		
		//TODO check and remove duplicates
		st.msi.termSourceName = mt.IDF.termSourceName;
		st.msi.termSourceURI = mt.IDF.termSourceFile;
		st.msi.termSourceVersion = mt.IDF.termSourceVersion;
		
		//TODO add samples...
		//get the nodes that have relevant sample information
		//e.g. characteristics 
		Collection<SDRFNode> samplenodes = new ArrayList<SDRFNode>();
		for (SDRFNode node : mt.SDRF.getNodes("sourcename")){
			samplenodes.add(node);
		}
		for (SDRFNode node : mt.SDRF.getNodes("samplename")){
			samplenodes.add(node);
		}
		for (SDRFNode node : mt.SDRF.getNodes("extractname")){
			samplenodes.add(node);
		}
		for (SDRFNode node : mt.SDRF.getNodes("labeledextractname")){
			samplenodes.add(node);
		}
		
		//now get nodes that are the topmost nodes
		ArrayList<SDRFNode> topnodes = new ArrayList<SDRFNode>();
		for (SDRFNode node : samplenodes ){
			if (node.getParentNodes().size() == 0){
				topnodes.add(node);
			}
		}
			

		getLog().info("Node names");
		//create a sample from each topmost node
		for(SDRFNode sdrfnode : topnodes ){
			getLog().info(sdrfnode.getNodeType());
			getLog().info(sdrfnode.getNodeName());
			
			SampleNode scdnode = new SampleNode();
			scdnode.setNodeName(sdrfnode.getNodeName());
			//since some attributes only exist for some sub-classes, need to test 
			//for instanceof for each of those sub-classes, cast accordingly
			//and then access the attributes
			List<CharacteristicsAttribute> characteristics  = null;
			Map<String, String> comments = null;
			if (sdrfnode instanceof uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode){
				//horribly long class references due to namespace collision
				uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode sdrfsamplenode = 
					(uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode) sdrfnode;
				scdnode.sampleDescription = sdrfsamplenode.description;	
				characteristics = sdrfsamplenode.characteristics;
				comments = sdrfsamplenode.comments;
			} else if (sdrfnode instanceof SourceNode){
				SourceNode sdrfsourcenode = (SourceNode) sdrfnode;
				scdnode.sampleDescription = sdrfsourcenode.description;
				characteristics = sdrfsourcenode.characteristics;
				comments = sdrfsourcenode.comments;
			} else if (sdrfnode instanceof ExtractNode){
				ExtractNode sdrfextractnode = (ExtractNode) sdrfnode;
				scdnode.sampleDescription = sdrfextractnode.description;
				characteristics = sdrfextractnode.characteristics;
				comments = sdrfextractnode.comments;
			} else if (sdrfnode instanceof LabeledExtractNode){
				LabeledExtractNode sdrflabeledextractnode = (LabeledExtractNode) sdrfnode;
				scdnode.sampleDescription = sdrflabeledextractnode.description;
				characteristics = sdrflabeledextractnode.characteristics;
				comments = sdrflabeledextractnode.comments;
			}
			
			if (characteristics != null){
				for (CharacteristicsAttribute sdrfcharacteristic : characteristics){
					CharacteristicAttribute scdcharacteristic = new CharacteristicAttribute();
					scdcharacteristic.type = sdrfcharacteristic.type;
					scdcharacteristic.setAttributeValue(sdrfcharacteristic.getAttributeValue());
					if (sdrfcharacteristic.unit != null){
						scdcharacteristic.unit = new UnitAttribute();
						scdcharacteristic.unit.termSourceREF = sdrfcharacteristic.unit.termSourceREF;
						scdcharacteristic.unit.termSourceID = sdrfcharacteristic.unit.termAccessionNumber;
					}
					scdcharacteristic.termSourceREF = sdrfcharacteristic.termSourceREF;
					scdcharacteristic.termSourceID = sdrfcharacteristic.termAccessionNumber;
					scdnode.addAttribute(scdcharacteristic);
				}
			}
			if (comments != null){
				for (String key:comments.keySet()){
					CommentAttribute comment = new CommentAttribute();
					comment.type = key;
					comment.setAttributeValue(comments.get(key));
					scdnode.addAttribute(comment);
				}
			}
			
			st.scd.addNode(scdnode);
		}
		
		return st;
	}
}
