package uk.ac.ebi.fgpt.sampletab.utils;


public class FileGlobIterable extends FileRegexIterable {
    
    public FileGlobIterable(String glob){
        super(globToRegex(glob));
    }

    public static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder(glob.length());
        for (char currentChar : glob.toCharArray()) {
            switch (currentChar) {
                case '*':
                    sb.append(".*");
                    break;
                case '.':
                    sb.append("\\.");
                    break;
                case '?':
                    sb.append("\\?");
                    break;
                default:
                    sb.append(currentChar);
                    break;
            }
        }
        return sb.toString();
    }

}