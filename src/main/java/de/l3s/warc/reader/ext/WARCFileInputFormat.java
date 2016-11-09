package de.l3s.warc.reader.ext;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WARCFileInputFormat extends FileInputFormat<String, Snapshot> {
    public enum StatusSelection {
        ALL, SUCCESS
    }

    private static final String URL_PATTERN = "subsetextractor.pattern.url";
    private static final String MIME_PATTERN = "subsetextractor.pattern.mime";
    private static final String STATUS_PATTERN = "subsetextractor.pattern.status";

    @Override
    public RecordReader<String, Snapshot> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        WARCFileRecordReader reader = new WARCFileRecordReader();
        reader.initialize(split, context);
        return reader;
    }


    @Override
    protected boolean isSplitable(JobContext context, Path filename) {

        return true;
    }

    public static String getUrlPattern(Configuration conf, String defaultValue) {
        return conf.get(URL_PATTERN, defaultValue);
    }

    public static void setUrlPattern(Configuration conf, String pattern) {
        conf.set(URL_PATTERN, pattern);
    }

    public static String getMimePattern(Configuration conf, String defaultValue) {
        return conf.get(MIME_PATTERN, defaultValue);
    }

    public static void setMimePattern(Configuration conf, String pattern) {
        conf.set(MIME_PATTERN, pattern);
    }

    public static void setStatusPattern(Configuration conf, StatusSelection selection) {
        conf.setEnum(STATUS_PATTERN, selection);
    }

    public static StatusSelection getStatusPattern(Configuration conf, StatusSelection defaultValue) {
        return conf.getEnum(STATUS_PATTERN, defaultValue);
    }

}
