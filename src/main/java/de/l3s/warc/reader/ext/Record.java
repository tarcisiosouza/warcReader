package de.l3s.warc.reader.ext;


public class Record {
    public String timestamp = null;
    public String redirectUrl = null;
    public String digest = null;
    public String originalUrl = null;
    public String string = null;
    public String getString() {
		return string;
	}
	public void setString(String string) {
		this.string = string;
	}
	public String mime = null;
    public int compressedSize = 0;
    public String meta = null;
    public String surtUrl = null;
    public int status = 0;
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getRedirectUrl() {
		return redirectUrl;
	}
	public void setRedirectUrl(String redirectUrl) {
		this.redirectUrl = redirectUrl;
	}
	public String getDigest() {
		return digest;
	}
	public void setDigest(String digest) {
		this.digest = digest;
	}
	public String getOriginalUrl() {
		return originalUrl;
	}
	public void setOriginalUrl(String originalUrl) {
		this.originalUrl = originalUrl;
	}
	public String getMime() {
		return mime;
	}
	public void setMime(String mime) {
		this.mime = mime;
	}
	public int getCompressedSize() {
		return compressedSize;
	}
	public void setCompressedSize(int compressedSize) {
		this.compressedSize = compressedSize;
	}
	public String getMeta() {
		return meta;
	}
	public void setMeta(String meta) {
		this.meta = meta;
	}
	public String getSurtUrl() {
		return surtUrl;
	}
	public void setSurtUrl(String surtUrl) {
		this.surtUrl = surtUrl;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
    
    
    
}
