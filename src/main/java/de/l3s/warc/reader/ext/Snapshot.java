package de.l3s.warc.reader.ext;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.archive.format.http.HttpHeader;
import org.archive.format.http.HttpHeaders;
import org.archive.format.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

public class Snapshot {
    private static final Logger logger = LoggerFactory.getLogger(Snapshot.class);
    private final InputStream content;
    private final HttpHeaders headers;
    private final String url;
    private final String timestamp;
    private final String mimeType;
    private final int status;
    private byte[] contentBuffer = null;

    public Snapshot(String url, String mimeType, String timestamp, HttpResponse response) {
        this.url = url;
        this.mimeType = mimeType;
        this.timestamp = timestamp;
        status = response.getMessage().getStatus();
        headers = response.getHeaders();
        content = response.getInner();
    }

    private static Map<String, String> asMap(HttpHeaders headers) {
        Map<String, String> ret = new HashMap<>(headers.size(), 1.0f);
        for (HttpHeader header : headers) {
            ret.put(header.getName(), header.getValue());
        }
        return ret;
    }

    public byte[] getBinaryContent(long maxBytes) throws IOException {
        if (contentBuffer == null) {
            contentBuffer = ByteStreams.toByteArray(ByteStreams.limit(content, maxBytes));
        }
        return contentBuffer;
    }

    public String getStringContent(long maxBytes) throws IOException {
        byte[] content = getBinaryContent(maxBytes);
        String charset = new OnlyHtmlCharsetDetector().getCharset(content, (int) Math.min(maxBytes, content.length), headers);
        return new String(content, charset);
    }

    public Map<String, String> getHeaders() {
        return asMap(headers);
    }

    public String getMimeType() {
        return mimeType;
    }

    public int getStatus() {
        return status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getUrl() {
        return url;
    }
}
