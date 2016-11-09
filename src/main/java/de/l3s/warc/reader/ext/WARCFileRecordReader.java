package de.l3s.warc.reader.ext;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.format.http.HttpResponse;
import org.archive.format.http.HttpResponseParser;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.util.ArchiveUtils;
import org.archive.util.MimetypeUtils;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.l3s.warc.reader.ext.WARCFileInputFormat.StatusSelection;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.format.http.HttpResponse;
import org.archive.format.http.HttpResponseParser;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.util.ArchiveUtils;
import org.archive.util.MimetypeUtils;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WARCFileRecordReader extends RecordReader<String, Snapshot> {
    public enum Status {
        /** Unparseable record */
        Skipped,
        /** Irrelevant WARC record type */
        SkippedWarcType,
        /** Irrelevant MIME type */
        SkippedMimeType,
        /** Irrelevant HTTP status */
        SkippedHttpStatus,
        /* Did not match URL regex */
        SkippedUrl,
        /** Matched all requirements */
        Matched,
        /** Exception occured during processing */
        Failed, InvalidDate
    }


    private static final Logger logger = LoggerFactory.getLogger(WARCFileRecordReader.class);
    private final static int GZIP_ID1 = 0x1f;
    private final static int GZIP_ID2 = 0x8b;
    private final static int GZIP_CM_DEFLATE = 0x08;
    private Iterator<ArchiveRecord> ar;
    private FSDataInputStream fsin;
    private ArchiveReader reader;
    private final HttpResponseParser responseParser = new HttpResponseParser();
    private Snapshot value;
    private long splitLength;
    private TaskAttemptContext context;
    private Pattern urlPattern;
    private Pattern mimePattern;
    private WARCFileInputFormat.StatusSelection statusSelection;
    private long start;

    public WARCFileRecordReader() {
        // default constructor for use in FileInputFormat
    }

    // Constructor for use by CombineFileInputFormat
    public WARCFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
     //   logger.info("Processing input file {}", split.getPath(index));
        initialize(context, split.getPath(index), split.getOffset(index), split.getLength(index));
        context.setStatus(String.format("Processing %s (%d/%d)", split.getPath(index), index, split.getNumPaths()));
    }

    private void initialize(TaskAttemptContext context, Path path, long start, long length) throws IOException {
        this.context = context;
        this.start = start;
        this.splitLength = length;
        Configuration conf = context.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        fsin = fs.open(path);
        BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(path), start, length);
       // logger.info("Using block locations {} on host {}", locations, getExternalAddress().getHostName());
        if (start > 0) {
            seekToGZIPHeader(fsin);
        }
        reader = ArchiveReaderFactory.get(path.getName(), fsin, true);
        ar = reader.iterator();

        String urlRegex = WARCFileInputFormat.getUrlPattern(conf, null);
        this.urlPattern = urlRegex != null ? Pattern.compile(urlRegex) : null;
        String mimeRegex = WARCFileInputFormat.getMimePattern(conf, ".*");
        this.mimePattern = Pattern.compile(mimeRegex);
        statusSelection = WARCFileInputFormat.getStatusPattern(conf, StatusSelection.ALL);
       // logger.info("Using URL pattern='{}', MIME pattern='{}', status selection={}", urlRegex, mimeRegex, statusSelection);
    }

    private enum State {
        START, ID1, ID2
    }

    private boolean seekToGZIPHeader(FSDataInputStream is) throws IOException {
        State state = State.START;
        int nextByte;
        while ((nextByte = is.read()) != -1) {
            switch (state) {
            case START:
                if (nextByte == GZIP_ID1) {
                    state = State.ID1;
                }
                break;
            case ID1:
                if (nextByte == GZIP_ID2) {
                    state = State.ID2;
                } else {
                    state = State.START;
                }
                break;
            case ID2:
                if (nextByte == GZIP_CM_DEFLATE) {
                    is.seek(is.getPos() - 3);
                    return true;
                } else {
                    state = State.START;
                }
                break;
            default:
                throw new IllegalStateException();
            }
        }
        return false;
    }


    private static InetAddress getExternalAddress() throws IOException {
        List<InetAddress> globalAddresses = new ArrayList<>();
        List<InetAddress> siteLocalAddresses = new ArrayList<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            for (InterfaceAddress address : iface.getInterfaceAddresses()) {
                InetAddress inetAddress = address.getAddress();
                if (inetAddress.isLinkLocalAddress() || inetAddress.isLoopbackAddress()) {
                    continue;
                }
                if (inetAddress.isSiteLocalAddress()) {
                    siteLocalAddresses.add(inetAddress);
                } else {
                    globalAddresses.add(inetAddress);
                }
            }
        }

        if (!globalAddresses.isEmpty()) {
            return globalAddresses.get(0);
        } else if (!siteLocalAddresses.isEmpty()) {
            return siteLocalAddresses.get(0);
        } else {
         //   logger.info("Could not find external InetAddress, returning localhost");
            return InetAddress.getLocalHost();
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        if (inputSplit instanceof FileSplit) {
            FileSplit split = (FileSplit) inputSplit;
            initialize(context, split.getPath(), split.getStart(), split.getLength());
        } else {
            throw new IllegalArgumentException("Not a FileSplit: " + inputSplit);
        }
    }

    @Override
    public void close() throws IOException {
        fsin.close();
        reader.close();
    }

    @Override
    public String getCurrentKey() {
        return value.getUrl();
    }

    @Override
    public Snapshot getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return ((fsin.getPos() - start) * 1.0f) / splitLength;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (fsin.getPos() < start + splitLength && ar.hasNext()) {
            try {
                ArchiveRecord record = ar.next();
                ArchiveRecordHeader header = record.getHeader();
                if (isHeaderRecord(header)) {
                    context.getCounter(Status.SkippedWarcType).increment(1);
                    continue;
                }
                String targetURI = header.getUrl();
                // apply regex to URL and skip (but count) unwanted URLs
                if (urlPattern != null && !urlPattern.matcher(targetURI).matches()) {
                    context.getCounter(Status.SkippedUrl).increment(1);
                    continue;
                }
                HttpResponse response = responseParser.parse(record);
                String contentType = MimetypeUtils.truncate(response.getHeaders().getValueCaseInsensitive("Content-Type"));
                if (!mimePattern.matcher(contentType).matches()) {
                    context.getCounter(Status.SkippedMimeType).increment(1);
           //         logger.debug("Skipping {} because MIME type {} does not match {}", targetURI, contentType, mimePattern);
                    continue;
                }
                if (statusSelection != WARCFileInputFormat.StatusSelection.ALL && response.getMessage().getStatus() >= 300) {
                    context.getCounter(Status.SkippedHttpStatus).increment(1);
                    continue;
                }
                String date = null;
                long timestamp;
                try {
                    if (header instanceof ARCRecordMetaData) {
                        date = ((ARCRecordMetaData) header).getDate();
                        if (date != null) {
                            if (date.length() == 14) {
                                timestamp = ArchiveUtils.parse14DigitDate(date).getTime();
                            } else {
                                timestamp = ArchiveUtils.parse12DigitDate(date).getTime();
                            }
                        } else {
                            timestamp = -1;
                        }
                    } else {
                        date = (String) header.getHeaderValue(WARCConstants.HEADER_KEY_DATE);
                        timestamp = date != null ? ISODateTimeFormat.dateTimeNoMillis().parseMillis(date) : -1;
                    }
                } catch (ParseException e) {
             //       logger.debug("Invalid date header '{}'", date, e);
                    context.getCounter(Status.InvalidDate).increment(1);
                    timestamp = -1;
                }
                this.value = new Snapshot(targetURI, contentType, String.valueOf(timestamp), response);
                return true;
            } catch (RuntimeException e) {
               // logger.warn("Could not read archive record, skipping", e);
            }
            context.getCounter(Status.Skipped).increment(1);
        }
        return false;
    }

    private boolean isHeaderRecord(ArchiveRecordHeader header) {
        if (header instanceof ARCRecordMetaData
                && (header.getUrl() == null || header.getUrl().startsWith("filedesc://"))) {
            return true;
        } else {
            String recordType = (String) header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);
            return recordType != null && !"response".equals(recordType);
        }
    }

}