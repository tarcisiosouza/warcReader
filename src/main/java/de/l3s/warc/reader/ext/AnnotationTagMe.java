package de.l3s.warc.reader.ext;

import java.util.List;

import it.enricocandino.tagme4j.TagMeClient;
import it.enricocandino.tagme4j.TagMeException;
import it.enricocandino.tagme4j.model.Annotation;
import it.enricocandino.tagme4j.response.TagResponse;

public class AnnotationTagMe {
	 private static TagMeClient tagMeClient;
	 
	public List<Annotation> annotate (String input) throws TagMeException
	{
		
		TagResponse tagResponse = tagMeClient
                .tag()
                .text(input)
                .longText(1)
                .lang("de")
                .epsilon(0.5F)
                .execute();
		
		return tagResponse.getAnnotations();
	}

	public AnnotationTagMe ()  {
		tagMeClient = new TagMeClient ("84fbb046-5277-4575-a6b6-3530af2f11ea-843339462");
		
	}
	
}
