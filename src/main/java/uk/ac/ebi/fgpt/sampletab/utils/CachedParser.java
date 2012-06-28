package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.util.concurrent.ExecutionException;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;

public class CachedParser {

    private static final LoadingCache<File, SampleData> cache = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .build(
            new CacheLoader<File, SampleData>() {
                public synchronized SampleData load(File file) throws ParseException {
                    SampleTabSaferParser parser = new SampleTabSaferParser();
                    return parser.parse(file);
                }
            });;
    
    public static SampleData get(File file) throws ParseException{
        try {
            return cache.get(file);
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new ParseException(e.getCause());
        }
    }
}
