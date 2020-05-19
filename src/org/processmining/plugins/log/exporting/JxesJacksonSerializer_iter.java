package org.processmining.plugins.log.exporting;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Date;
import java.util.ListIterator;

import org.apache.commons.lang3.time.FastDateFormat;
import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.logging.XLogging;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContainer;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeList;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.out.XSerializer;
import org.deckfour.xes.util.XRuntimeUtils;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * XES serialization to JXES including all t
import com.google.gson.JsonObject;race/event attributes.
 *
 * @author Hossameldin Khalifa
 *
 */
public final class JxesJacksonSerializer_iter implements XSerializer {

	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");

	
	/*
	 * (non-Javadoc)
	 *
	 * @see org.deckfour.xes.out.XesSerializer#getDescription()
	 */
	public String getDescription() {
		return "XES JXES Serialization";
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.deckfour.xes.out.XesSerializer#getName()
	 */
	public String getName() {
		return "XES JXES";
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.deckfour.xes.out.XesSerializer#getAuthor()
	 */
	public String getAuthor() {
		return "Hossameldin Khalifa";
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.deckfour.xes.out.XesSerializer#getSuffices()
	 */
	public String[] getSuffices() {
		return new String[] { "json" };
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.deckfour.xes.out.XesSerializer#serialize(org.deckfour.xes.model.XLog,
	 * java.io.OutputStream)
	 */
	public void serialize(XLog log, OutputStream out) throws IOException {
		XLogging.log("start serializing log to .json (Jsoniter) ", XLogging.Importance.DEBUG);
		long start = System.currentTimeMillis();


		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		JsonFactory jfactory = new JsonFactory();
		JsonGenerator writer = jfactory
		  .createGenerator(stream, JsonEncoding.UTF8);
		
		
//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//
//			}});
//		System.addErr(err);
		
		
		//begin json output
		writer.writeStartObject();
		
		
		
		// add json key:
		writer.writeFieldName("log-attrs");
		
		//begin log-attrs object 
		writer.writeStartObject();
		
		//add xes version and features
		writer.writeStringField("xes.version",XRuntimeUtils.XES_VERSION);
		writer.writeStringField("xes.features","nested-attributes");
		writer.writeStringField("openxes.version",XRuntimeUtils.OPENXES_VERSION);

		
		
		//end log-attrs object
		writer.writeEndObject(); 
		writer.writeEndObject();
		
		//begin log-children object
		writer.writeFieldName("log-children");
		writer.writeStartObject();
	

		// iterate over all log attributes
		for (XAttribute attr : log.getAttributes().values()) {
			addAttr(attr,writer);
		}

		// end log-children object
		writer.writeEndObject();


		//
		
		// begin object global
		writer.writeFieldName("global");
		writer.writeStartObject();
		
		//begin object trace
		writer.writeFieldName("trace");
		writer.writeStartObject();
		
		//add global trace attributes
		for (XAttribute attr : log.getGlobalTraceAttributes()) {
			addAttr(attr,writer);
		}
	
		// end object trace
		writer.writeEndObject();
		
		
		//begin object event
		writer.writeFieldName("event");
		writer.writeStartObject();

		//add global event attributes
		for (XAttribute attr : log.getGlobalEventAttributes()) {
			addAttr(attr,writer);
		}
		// end object event
		writer.writeEndObject();

		// end global object
		writer.writeEndObject();



		//begin extensions array [
		writer.writeFieldName("extensions");
		writer.writeStartArray();
		for (XExtension extension : log.getExtensions()) {
			// begin object for every new extension {
			writer.writeStartObject();

			// set the name, prefix and uri for every extension
			writer.writeStringField("name",extension.getName());
		
			writer.writeStringField("prefix",extension.getPrefix());
			
			writer.writeStringField("uri",extension.getUri().toString());

			// end object for every new extension }
			writer.writeEndObject();
		}

		// end extensions array ]
		writer.writeEndArray();


		//begin classifiers object {
		writer.writeFieldName("classifiers");
		writer.writeStartObject();

		//iterate over all event classifiers 
		for (XEventClassifier classifier : log.getClassifiers()) {

			if (classifier instanceof XEventAttributeClassifier) {
				
				// get classifier object to exract names and values
				XEventAttributeClassifier attrClass = (XEventAttributeClassifier) classifier;
				
				// add name and begin array for every classifier [
				writer.writeFieldName(attrClass.name());
				writer.writeStartArray();
				
				// get and iterate on classifier key
				String[] myArray = attrClass.getDefiningAttributeKeys();
				for (int i = 0; i < myArray.length; i++) {
					// add key to array
					writer.writeString(myArray[i]);
			    }

				writer.writeEndArray();
			}
		}
		//end classifiers object }
		writer.writeEndObject();



		// begin traces array
		writer.writeFieldName("traces");
		writer.writeStartArray();

		// add all traces
		for (XTrace trace : log) {
			compileTrace(trace,writer);
		}
		
		// end traces array
		writer.writeEndArray();
		
		// end the whole object
		writer.writeEndObject();

		writer.close();
		out.close();
		String duration = " (" + (System.currentTimeMillis() - start) + " msec.)";
		XLogging.log("finished serializing log" + duration, XLogging.Importance.DEBUG);
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
	}

	private void compileTrace(XTrace trace,JsonGenerator writer) throws IOException{
		// begin trace {
		writer.writeStartObject();
		
		// add trace attrs
		writer.writeFieldName("attrs");
		writer.writeStartObject();
		// iterate over all trace attirbutes
		for (XAttribute attr : trace.getAttributes().values()){
			addAttr(attr,writer);
		}
		// end trace attributes
		writer.writeEndObject();
		
		// add trace events
		writer.writeFieldName("events");
		writer.writeStartArray();

		// iterate over all events in this trace
		for (ListIterator<XEvent> iterator = trace.listIterator(); iterator.hasNext();) {
			// begin event 
			writer.writeStartObject();
			XEvent event = iterator.next();
			for (XAttribute attr : event.getAttributes().values()) {
				addAttr(attr,writer);
			}
			//end event 
			writer.writeEndObject();
		}
		
		//end events array
		writer.writeEndArray();

		//end trace object
		writer.writeEndObject();
		
	}


	




	protected void addAttr(XAttribute attribute, JsonGenerator json) throws IOException{
	
		// always set the name as first thing
		String key = attribute.getKey();

		// check if we are dealing with a nested attr
		boolean nested = attribute.hasAttributes();
		if(nested){
			json.writeFieldName(key);
			json.writeStartObject();
			key = "value";
		}

		if (attribute instanceof XAttributeTimestamp) {
			Date timestamp = ((XAttributeTimestamp) attribute).getValue();
			json.writeStringField(key, dateFormat.format(timestamp));

		} else if ( attribute instanceof XAttributeContinuous ) {
			json.writeNumberField(key, ((XAttributeContinuous) attribute).getValue());
		} else if (attribute instanceof XAttributeDiscrete) {
			json.writeNumberField(key, ((XAttributeDiscrete) attribute).getValue());
		} else if (attribute instanceof XAttributeBoolean ) {
			json.writeBooleanField(key, ((XAttributeBoolean) attribute).getValue());
		} else if (attribute instanceof XAttributeList ) {
			// get XAttributeList
			Collection<XAttribute> list = ((XAttributeList) attribute).getCollection();
			// begin the json array [
			json.writeStartArray();
			// for every attribute in the list create a json object
			for (XAttribute attr : list) {
				// begin the object {
				json.writeStartObject();
				// run the method with the attribute
				addAttr(attr,json);
				// end the object }
				json.writeEndObject();
			}
			//end the json array ]
			json.writeEndArray();
		
		} else if (attribute instanceof XAttributeContainer ) {
			Collection<XAttribute> container = ((XAttributeContainer) attribute).getCollection();
			json.writeStartObject();
			for (XAttribute attr : container) {
				addAttr(attr,json);
			}
			json.writeEndObject();
		} else {
			json.writeStringField(key,attribute.toString());
		}


		if(nested){
			// add nested attributes
			json.writeFieldName("nested-attrs");
			// begin nested-attributes object
			json.writeStartObject();
			for(XAttribute attr : attribute.getAttributes().values()) {
				addAttr(attr,json);
			}
			// end nested-attributes object }
			json.writeEndObject();

			//end the whole object opened in the begining of the method
			json.writeEndObject();
		}

	}




	/**
	 * toString() defaults to getName().
	 */
	public String toString() {
		return this.getName();
	}

}