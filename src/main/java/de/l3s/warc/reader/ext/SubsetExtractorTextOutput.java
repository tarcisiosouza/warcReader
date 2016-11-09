package de.l3s.warc.reader.ext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat.ParseException;
import com.google.common.net.MediaType;
import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.HtmlArticleExtractor;
import de.unihd.dbs.heideltime.standalone.HeidelTimeStandalone;



import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class SubsetExtractorTextOutput extends Configured implements Tool {

	private static String format;
	private static Configuration conf;
	private static HeidelTimeStandalone heidelTime;
    public static class CliArguments {

        @Argument(required = true, index = 0, metaVar = "path")
        String inputPath;

    //    @Argument(required = true, index = 1, metaVar = "path")
      //  String outputPath;

        
    //    @Option(name = "-url", metaVar = "regex", usage = "Java regular expression for the URL")
      //  String urlPattern = ".*";

        
        @Option(name = "-mime", metaVar = "regex", usage = "Java regular expression for the MIME type")
        String mimePattern = null;

        @Option(name = "-status", metaVar = "codePattern", usage = "")
        WARCFileInputFormat.StatusSelection statusPattern = WARCFileInputFormat.StatusSelection.ALL;
    }

    /**
     * Match URLs against the given regex and output matches.
     */
    public static class CalendarUtils {

        public static String dateFormat = "yyyyMMddHHmmss";
        private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);

        public static String ConvertMilliSecondsToFormattedDate(String milliSeconds){
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.parseLong(milliSeconds));
            return simpleDateFormat.format(calendar.getTime());
        }
    }
    
    public static class IndexerMapper extends Mapper<String, Snapshot, NullWritable, Text> {

        public enum Status {
            Matched, Failed
        }

        private String str;
        private int count;
        private HtmlUtils html;
        private static BoilerpipeExtractor extractor;
    	private static String article;
    	private static MapWritable doc;
    	private static HtmlArticleExtractor htmlExtr;
    	private URL Url;
        private String[] allMatches = new String[1];
        private static HashMap<String,String> domains = new HashMap<String, String>();
        private static HashMap<String,String> articles = new HashMap<String, String>();
        private static final Logger logger = LoggerFactory.getLogger(SubsetExtractorTextOutput.IndexerMapper.class);
        /** JSON generator */
        private final ObjectMapper jsonMapper = new ObjectMapper();
        /** Output key (null value), reused */
        private final NullWritable outKey = NullWritable.get();
        /** Output value container, reused */
        private final Text outValue = new Text();

        /**
         * Process a record (called for every record).
         *
         * @param key
         *            WARC file offset, ignored
         * @param value
         *            WARC record (WARC header + HTTP header + content)
         * @param context
         *            reference to Hadoop runtime
         */
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException 
        {
        	
        	
        	count = 0;
        	extractor = CommonExtractors.ARTICLE_EXTRACTOR;
   		 	htmlExtr = HtmlArticleExtractor.INSTANCE;
        	html = new HtmlUtils ();
//    		 Path location = new Path("/Users/tarcisio/Documents/Promotion/german_news.txt");
       // 	Path location = new Path("/tarcisio/input/german_news.txt");
        	Path location = new Path("/user/souza/uk_domains.txt");
    		    FileSystem fileSystem = location.getFileSystem(context.getConfiguration());
    		    
    			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
    //		            new Path("/Users/tarcisio/Documents/Promotion/german_news.txt"), true);
    					new Path("/user/souza/uk_domains.txt"), true);
//    					new Path("/tarcisio/input/german_news.txt"), true);
    	        
    			while(fileStatusListIterator.hasNext())
    			{
    		    	
    				String line;
    		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
    		        BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));		    
    		       
    		        while ((line = br.readLine()) != null) 
    	            {
    		        	
    		        	domains.put(line,line);
    		        	
    	            }
    			}
        	 
        }
        @Override
        protected void map(String key, Snapshot record, Context context)
                throws IOException, InterruptedException {

        	String date_converted = null;
       	if (record.getStatus() != 200)
        		return;
        	try {
        	String domain = getDomain(record.getUrl());
        
        	if (domains.containsKey(domain))
        	{
            try {
            	count ++;
            	
            	if (count >10)
            		return;
            	
            	doc = new MapWritable();
	         	
                // update statistics
                context.getCounter(Status.Matched).increment(1);
                context.getCounter("Mime type", normalizedMimeType(record.getMimeType())).increment(1);
                context.getCounter("HTTP status", String.valueOf(record.getStatus())).increment(1);
                /*
                Map<String, Object> output = new HashMap<>();
                output.put("url", record.getUrl());
                output.put("ts", record.getTimestamp());
                output.put("c", record.getStringContent(1024 * 1024));
                output.put("type", record.getMimeType());
                output.put("s", record.getStatus());
*/
        //        System.out.println(record.toString());
         
              date_converted = CalendarUtils.ConvertMilliSecondsToFormattedDate(record.getTimestamp());
              doc.put(new Text("ts"), new Text(date_converted));
           
	         	
              String text = date_converted + "\n";
                // do some processing with the record
                //String json = jsonMapper.writeValueAsString(output);

                // update output value container
               // outValue.set(json);

                // report result back to Hadoop
              //  context.write(outKey, outValue);
              
	         	try {
	         		article = extractor.getText(record.getStringContent(1024 * 1024));
	         	
				} catch (Exception e) {
					return;
				}

            } catch (Exception e) {
                // add some context to exceptions
               // logger.info("Exception while mapping record {}", key, e);
                context.getCounter(Status.Failed).increment(1);
            }
            String title = getTitle(record.getStringContent(1024 * 1024));
          //  String links = html.exec(record.getStringContent(1024 * 1024));
            doc.put(new Text("title"),new Text(title));
            doc.put(new Text("text"), new Text(article));
            if (wordCount(article).contentEquals("0"))
            	return;
            else
            {
         //   doc.put(new Text("wc"), new Text(wordCount(article)));
            	
           // String date = getPubDate (record.getStringContent(1024 * 1024),domain);
           // if (date.contentEquals(""))
           // 	return;
           // if (date.contentEquals(""))
           // String	date = "19700101010100";
           // doc.put(new Text("pubDate"), new Text (date));
           // doc.put(new Text ("html"), new Text(record.getStringContent(1024 * 1024)));
            }
            //doc.put(new Text("links"), new Text(links));
            String text = date_converted + "\n"+ article;
            context.write(NullWritable.get(),new Text (text));
            
        }
        } catch (Exception e)
        {	
        	return;
        }
        }
       
  
        public String getDomain (String url) throws MalformedURLException
    	{
    		Matcher m = Pattern.compile("(http).*").matcher(url);
    		while (m.find()) 
            {
    			
    			allMatches[0] = m.group(); 
            	str = allMatches[0];
            	Url = new URL(str);
            }
            
    		String Domain = Url.getHost();
    		if (Domain.contains("www")) {
    			int index = Domain.indexOf(".");
    			Domain = Domain.substring(index + 1, Domain.length());
    		}
    		
    		return Domain;
    		
    	}
        private String normalizedMimeType(String mimeType) {
            try {
                return MediaType.parse(mimeType).type();
            } catch (IllegalArgumentException e) {
         //       logger.debug("Not a valid MIME type: '{}'", mimeType);
                return "unknown";
            }
        }
        
        public static String convertToDate(String input) throws java.text.ParseException, ParseException {
            Date date = null;
            
            if(null == input) {
                return null;
            }
            
              ArrayList<SimpleDateFormat>  dateFormats = new ArrayList<SimpleDateFormat>() {/**
    			 * 
    			 */
    			private static final long serialVersionUID = 1L;

    		{
                add(new SimpleDateFormat("M/dd/yyyy"));
                add(new SimpleDateFormat("dd/MM/yyyy"));
                add(new SimpleDateFormat("dd.M.yyyy"));
                add(new SimpleDateFormat("M/dd/yyyy hh:mm:ss a"));
                add(new SimpleDateFormat("dd.M.yyyy hh:mm:ss a"));
                add(new SimpleDateFormat("dd.MMM.yyyy"));
                add(new SimpleDateFormat("dd-MMM-yyyy"));
                add(new SimpleDateFormat("dd-MM-yyyy"));
                add(new SimpleDateFormat("yyyyMMdd"));
//                add(new SimpleDateFormat("yyyy-MM"));
                add(new SimpleDateFormat("yyyy-MM-dd"));
                add(new SimpleDateFormat("dd.MM.yyyy"));
                add(new SimpleDateFormat("yyyy/MM/dd"));
                add(new SimpleDateFormat("yyyy.MM.dd"));
              }
              };
            for (SimpleDateFormat form : dateFormats) {
                form.setLenient(false);
                try {
                	date = form.parse(input);	
                	format = form.toPattern().toString();
                } catch (Exception e)
                {
                	continue;
                }
    			
            }
            if (date == null)
            	return "";
            
            return date.toString();
        }
    	private static String[] getDate(String desc) {
  		  int count=0;
  		  String[] allMatches = new String[1];
//  		  Matcher m = Pattern.compile("(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](19)[9]\\d").matcher(desc);
  		  Matcher m = Pattern.compile("(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](19)\\d\\d").matcher(desc);
//  		  Matcher m1 = Pattern.compile("(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](20)[01]\\d").matcher(desc);
  		  Matcher m1 = Pattern.compile("(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](20)\\d\\d").matcher(desc);
//  		  Matcher m2 = Pattern.compile("(19)[9]\\d[- /.](0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])").matcher(desc);
  		  Matcher m2 = Pattern.compile("(19)\\d\\d[- /.](0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])").matcher(desc);
//  		  Matcher m3 = Pattern.compile("(20)[01]\\d[- /.](0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])").matcher(desc);
  		  Matcher m3 = Pattern.compile("(20)\\d\\d[- /.](0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|[12][0-9]|3[01])").matcher(desc);
//  		  Matcher m4 = Pattern.compile("(19)[9]\\d(0[1-9]|[12][0-9]|3[01])(0[1-9]|[12][0-9]|3[01])").matcher(desc);
  		  Matcher m4 = Pattern.compile("(19)\\d\\d(0[1-9]|[12][0-9]|3[01])(0[1-9]|[12][0-9]|3[01])").matcher(desc);
//  		  Matcher m5 = Pattern.compile("(20)[01]\\d(0[1-9]|[12][0-9]|3[01])(0[1-9]|[12][0-9]|3[01])").matcher(desc);
  		  Matcher m5 = Pattern.compile("(20)\\d\\d(0[1-9]|[12][0-9]|3[01])(0[1-9]|[12][0-9]|3[01])").matcher(desc);
//  		  Matcher m6 = Pattern.compile("(19)[9]\\d[- /.](0[1-9]|[12][0-9])").matcher(desc);
  		  Matcher m6 = Pattern.compile("(19)\\d\\d[- /.](0[1-9]|[12][0-9])").matcher(desc);
//  		  Matcher m7 = Pattern.compile("(20)[01]\\d[- /.](0[1-9]|[12][0-9])").matcher(desc);
  		  Matcher m7 = Pattern.compile("(20)\\d\\d[- /.](0[1-9]|[12][0-9])").matcher(desc);
  		  if (m.find()) {
  		    allMatches[count] = m.group();
  		    return allMatches;
  		  }
  		  
  		  if (m1.find()) {
  			    allMatches[count] = m1.group();
  			    return allMatches;
  		  }
  		  
  		  if (m2.find()) {
  			    allMatches[count] = m2.group();
  			    return allMatches;
  			    
  			  }
  		  
  		  if (m3.find()) {
  			    allMatches[count] = m3.group();
  			    return allMatches;
  			    
  			  }
  		  if (m4.find()) {
  			  	
  			    allMatches[count] = m4.group();
  			    return allMatches;
  		  }
  		  if (m5.find()) {
  			  	
  			    allMatches[count] = m5.group();
  			    return allMatches;
  		  }
  		  if (m6.find()) {
  			  	
  			    allMatches[count] = m6.group();
  			    return allMatches;
  		  }
  		  
  		  if (m7.find()) {
  			  	
  			    allMatches[count] = m7.group();
  			    return allMatches;
  		  }
  		  return allMatches;
  		}
    }
 
    public static String getTitle (String input)
    {
    	
    	try{
			Document doc = Jsoup.parse(input);
			String title = doc.title();
			return title;
		} catch (Exception e)
		{
			return ("");
			
		}
    	
    }
    public static String wordCount (String str)
    {
    	int count;
    	try{
			StringTokenizer token = new StringTokenizer (str.toString());
			count = 0;
			while (token.hasMoreTokens())
			{
				token.nextToken();
				count++;
			}
			
			return (Integer.toString(count));
		} catch (Exception e)
		{
			
			return ("0");
			
		}
    }

    /**
     * Setup and run the job.
     */
    @Override
    public int run(String[] args) throws Exception {
       
        Job job = Job.getInstance(getConf());
        //Job job = new Job (super.getConf());
        job.setJobName("elasticsearch-index-warc-uk-news");
        job.setJarByClass(SubsetExtractorTextOutput.class);
        job.setMapperClass(IndexerMapper.class);
        // default reducer just copies input to output
        job.setNumReduceTasks(100);
     
        conf = job.getConfiguration();
      
//        conf.set("es.nodes","localhost");
    //    conf.set("es.nodes","master02.ib");
        conf.setLong(YarnConfiguration.NM_PMEM_MB, 58000);
        conf.setLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 3000);
        conf.setLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 58000);
        conf.setLong(MRJobConfig.MAP_MEMORY_MB, 3000);
        conf.setLong(MRJobConfig.REDUCE_MEMORY_MB, 6000);
        conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx2400m");
        conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx4800m");
        conf.setLong("yarn.app.mapreduce.am.resource.mb", 10000);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx8000m");
        conf.setLong(MRJobConfig.IO_SORT_MB, 1000);

        conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 5);
        conf.setInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, 10);
        conf.setInt(MRJobConfig.JVM_NUMTASKS_TORUN, 200);
        conf.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 5);
       // conf.setInt("yarn.app.mapreduce.am.resource.mb", 10_000);
      //  conf.set("es.resource", "souza_warc_news_uk/capture");
       // conf.set("es.mapping.id", "id");
        //conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        //conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //conf.set("es.input.json", "yes");
        //conf.set("shield.user", "souza:pri2006");
      //  conf.set(k	"es.net.http.auth.user", "souza");
       // conf.set("es.net.http.auth.pass", "pri2006");
  //      conf.set("es.batch.size.entries","1");
//        conf.set("es.port", "9200");
   //     conf.set("es.port", "9250");
        conf.setBoolean("mapreduce.job.user.classpath.first", true);
        // set input/output directories in HDFS, output path must be non-existent
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);

        // set up compression of output files
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // configure reader and writer implementations
        job.setInputFormatClass(WARCFileInputFormat.class);
     // Output
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setSpeculativeExecution(false);
       // job.setMapOutputValueClass(MapWritable.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        // specify output of mapper
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
        	hdfs.delete(outputDir, true);
              // start job and wait for it to complete while printing progress information
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // use Hadoop runner to process default arguments
        int res = ToolRunner.run(conf,new SubsetExtractorTextOutput(), args);
        System.exit(res);
    }
}
