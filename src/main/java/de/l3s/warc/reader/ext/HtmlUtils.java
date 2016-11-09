package de.l3s.warc.reader.ext;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

public class HtmlUtils 
{
	
	private HashMap<String,String> hashLinks = new HashMap<String,String>();
	
	public String exec(String input) throws IOException {
		
		if (input == null || input.length()==0)
			return null;
		
		try{
			Document doc = Jsoup.parse(input.toString());
			String totalLinks = "";
			Elements links = doc.select("a[href]");
			for (Element link : links) {
				if (link.attr("abs:href").contains("http"))
					hashLinks.put(link.attr("abs:href"), "1");    
	        }
			
			Set<String> keys = hashLinks.keySet();

			for (String key : keys) {
				
				totalLinks = totalLinks + "\n" + key;
			}
			return totalLinks;
		} catch (Exception e)
		{
			
			throw new IOException("Caught exception processing input row ", e);
			
		}
		
	}
	
	
}

