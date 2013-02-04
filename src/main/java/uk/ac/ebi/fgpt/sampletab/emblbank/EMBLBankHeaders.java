package uk.ac.ebi.fgpt.sampletab.emblbank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMBLBankHeaders {

    public final List<String> headers;
    public final int doiindex;
    public final int pubmedindex;
    public final int projheaderindex;
    public final int pubheaderindex;
    public final int collectedbyindex;
    public final int identifiedbyindex;
    public final int taxidindex;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public EMBLBankHeaders(String[] line) throws IOException{

        this.headers = new ArrayList<String>();
        for (String header : line){
            //barcode has all headers prefixed with V_ for some reason...
            if (header.startsWith("V_")){
                header = header.substring(2);
            }
            log.info("Found header : "+header);
            headers.add(header);
        }
        
        this.doiindex = this.headers.indexOf("DOI");
        if (this.doiindex < 0){
            throw new IOException("Headers does not contain DOI");
        }
        this.pubmedindex = this.headers.indexOf("PUBMED_ID");
        if (this.pubmedindex < 0){
            throw new IOException("Headers does not contain PUBMED_ID");
        }
        this.projheaderindex = this.headers.indexOf("PROJECT_ACC");
        //barcode does not have project acc
        this.pubheaderindex = this.headers.indexOf("ENA_PUBID");
        if (this.pubheaderindex < 0){
            throw new IOException("Headers does not contain ENA_PUBID");
        }
        this.collectedbyindex = this.headers.indexOf("COLLECTED_BY");
        if (this.collectedbyindex < 0){
            throw new IOException("Headers does not contain COLLECTED_BY");
        }
        this.identifiedbyindex = this.headers.indexOf("IDENTIFIED_BY");
        if (this.identifiedbyindex < 0){
            throw new IOException("Headers does not contain IDENTIFIED_BY");
        }
        this.taxidindex = this.headers.indexOf("TAX_ID");
        if (this.taxidindex < 0){
            throw new IOException("Headers does not contain TAX_ID");
        }
        
    }

    public int size() {
        return headers.size();
    }

    public String get(int i) {
        return headers.get(i);
    }
}
