package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;

public class MageTabToSampleTab {
	
	private static final MageTabToSampleTab instance = new MageTabToSampleTab();
	public static final MAGETABParser<MAGETABInvestigation> parser = new MAGETABParser<MAGETABInvestigation>();

	private MageTabToSampleTab(){
		//private constructor to prevent accidental multiple initialisations
	}
	 
    public static MageTabToSampleTab getInstance() {
            return instance;
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
		MAGETABInvestigation mt = parser.parse(dataIn);
		//return null;
		return convert(mt);
	}
		
	public SampleData convert(MAGETABInvestigation mt){
		
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
		
		return st;
	}
}
