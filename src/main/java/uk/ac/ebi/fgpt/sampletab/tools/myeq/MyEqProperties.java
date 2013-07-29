package uk.ac.ebi.fgpt.sampletab.tools.myeq;

public class MyEqProperties {

    //TODO move all of this to a properties file
    
    private static String BIOSAMPLESSAMPLESSERVICE = "ebi.biosamples.samples";
    private static String BIOSAMPLESGROUPSSERVICE = "ebi.biosamples.groups";
    private static String ENASAMPLESSERVICE = "ebi.ena.samples";
    private static String ENAGROUPSSERVICE = "ebi.ena.groups";
    private static String ARRAYEXPRESSGROUPSERVICE = "ebi.arrayexpress.groups";
    private static String ARRAYEXPRESSSAMPLESERVICE = "ebi.arrayexpress.samples";
    private static String PRIDESAMPLESERVICE = "ebi.pride.samples";
    private static String COSMICSAMPLESERVICE = "sanger.cosmic.samples";

    private static boolean isSetup = false;
    
    public static String getBioSamplesSamplesService() {
        setup();
        return BIOSAMPLESSAMPLESSERVICE;
    }
    
    public static String getBioSamplesGroupsService() {
        setup();
        return BIOSAMPLESGROUPSSERVICE;
    }
    
    public static String getENASamplesService() {
        setup();
        return ENASAMPLESSERVICE;
    }
    
    public static String getENAGroupsService() {
        setup();
        return ENAGROUPSSERVICE;
    }
    
    public static String getArrayExpressSamplesService() {
        setup();
        return ARRAYEXPRESSSAMPLESERVICE;
    }
    
    public static String getArrayExpressGroupsService() {
        setup();
        return ARRAYEXPRESSGROUPSERVICE;
    }
    
    public static String getPrideSamplesService() {
        setup();
        return PRIDESAMPLESERVICE;
    }
    
    public static String getCosmicSamplesService() {
        setup();
        return COSMICSAMPLESERVICE;
    }
    
    private static void setup() {
        if (isSetup) return;
        isSetup = true;
    }
    
}
