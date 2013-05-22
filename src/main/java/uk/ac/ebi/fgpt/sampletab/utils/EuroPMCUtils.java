package uk.ac.ebi.fgpt.sampletab.utils;

import java.util.List;

import uk.ac.ebi.fgpt.sampletab.utils.europmc.ws.QueryException_Exception;
import uk.ac.ebi.fgpt.sampletab.utils.europmc.ws.ResponseWrapper;
import uk.ac.ebi.fgpt.sampletab.utils.europmc.ws.Result;
import uk.ac.ebi.fgpt.sampletab.utils.europmc.ws.WSCitationImpl;
import uk.ac.ebi.fgpt.sampletab.utils.europmc.ws.WSCitationImplService;


public class EuroPMCUtils {
    
    private static WSCitationImplService wSCitationImplService = new WSCitationImplService();
    private static WSCitationImpl wSCitationImpl = wSCitationImplService.getWSCitationImplPort();
    
    
    public static String getTitleByPUBMEDid(Integer pubmed) throws QueryException_Exception {
        String query = pubmed.toString();

        String dataSet = "metadata";
        String resultType = "lite";
        Integer offSet = 0;
        Boolean synonym = false;
        String email = null;
        ResponseWrapper results = wSCitationImpl.searchPublications(query,
                dataSet, resultType, offSet, synonym, email);
        List<Result> resultList = results.getResultList().getResult();
        if (resultList.size() > 0) {
            return results.getResultList().getResult().get(0).getTitle();
        } else {
            return null;
        }
    }
    
}
