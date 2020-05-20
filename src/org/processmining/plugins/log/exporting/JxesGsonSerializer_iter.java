package org.processmining.plugins.log.exporting;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

import com.google.gson.stream.JsonWriter;

/**
 * XES serialization to JXES including all t
import com.google.gson.JsonObject;race/event attributes.
 *
 * @author Hossameldin Khalifa
 *
 */
public final class JxesGsonSerializer_iter implements XSerializer {

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
		XLogging.log("start serializing log to .json", XLogging.Importance.DEBUG);
		long start = System.currentTimeMillis();
		OutputStreamWriter output_stream = new OutputStreamWriter(out);
		JsonWriter writer = new JsonWriter(output_stream);

//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//
//			}});
//		System.addErr(err);
		

		//begin json output
		writer.beginObject();
		
		// add json key:
		writer.name("log-properties");
		
		//begin log-attrs object 
		writer.beginObject();
		
		//add xes version and features
		writer.name("xes.version").value(XRuntimeUtils.XES_VERSION);
		writer.name("xes.features").value("nested-attributes");
		writer.name("openxes.version").value(XRuntimeUtils.OPENXES_VERSION);
		
		//end log-attrs object
		writer.endObject(); 
		
		
		//begin log-children object
		writer.name("log-attrs");
		writer.beginObject();
	

		// iterate over all log attributes
		for (XAttribute attr : log.getAttributes().values()) {
			addAttr(attr,writer);
		}

		// end log-children object
		writer.endObject();


		//
		
		// begin object global
		writer.name("global");
		writer.beginObject();
		
		//begin object trace
		writer.name("trace");
		writer.beginObject();
		
		//add global trace attributes
		for (XAttribute attr : log.getGlobalTraceAttributes()) {
			addAttr(attr,writer);
		}
	
		// end object trace
		writer.endObject();
		
		
		//begin object event
		writer.name("event");
		writer.beginObject();

		//add global event attributes
		for (XAttribute attr : log.getGlobalEventAttributes()) {
			addAttr(attr,writer);
		}
		// end object event
		writer.endObject();

		// end global object
		writer.endObject();



		//begin extensions array [
		writer.name("extensions");
		writer.beginArray();
		for (XExtension extension : log.getExtensions()) {
			// begin object for every new extension {
			writer.beginObject();

			// set the name, prefix and uri for every extension
			writer.name("name").value(extension.getName());
			writer.name("prefix").value(extension.getPrefix());
			writer.name("uri").value(extension.getUri().toString());

			// end object for every new extension }
			writer.endObject();
		}

		// end extensions array ]
		writer.endArray();


		//begin classifiers object {
		writer.name("classifiers");
		writer.beginObject();

		//iterate over all event classifiers 
		for (XEventClassifier classifier : log.getClassifiers()) {

			if (classifier instanceof XEventAttributeClassifier) {
				
				// get classifier object to exract names and values
				XEventAttributeClassifier attrClass = (XEventAttributeClassifier) classifier;
				
				// add name and begin array for every classifier [
				writer.name(attrClass.name());
				writer.beginArray();
				
				// get and iterate on classifier key
				String[] myArray = attrClass.getDefiningAttributeKeys();
				for (int i = 0; i < myArray.length; i++) {
					// add key to array
					writer.value(myArray[i]);
			    }

				writer.endArray();
			}
		}
		//end classifiers object }
		writer.endObject();



		// begin traces array
		writer.name("traces");
		writer.beginArray();

		// add all traces
		for (XTrace trace : log) {
			compileTrace(trace,writer);
		}
		
		// end traces array
		writer.endArray();
		
		// end the whole object
		writer.endObject();

		writer.close();
		out.close();
		String duration = " (" + (System.currentTimeMillis() - start) + " msec.)";
		XLogging.log("finished serializing log" + duration, XLogging.Importance.DEBUG);
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
	}

	private void compileTrace(XTrace trace,JsonWriter writer) throws IOException{
		// begin trace {
		writer.beginObject();
		
		// add trace attrs
		writer.name("attrs");
		writer.beginObject();
		// iterate over all trace attirbutes
		for (XAttribute attr : trace.getAttributes().values()){
			addAttr(attr,writer);
		}
		// end trace attributes
		writer.endObject();
		
		// add trace events
		writer.name("events");
		writer.beginArray();

		// iterate over all events in this trace
		for (ListIterator<XEvent> iterator = trace.listIterator(); iterator.hasNext();) {
			// begin event 
			writer.beginObject();
			XEvent event = iterator.next();
			for (XAttribute attr : event.getAttributes().values()) {
				addAttr(attr,writer);
			}
			//end event 
			writer.endObject();
		}
		
		//end events array
		writer.endArray();

		//end trace object
		writer.endObject();
		
	}


	




	protected void addAttr(XAttribute attribute, JsonWriter json) throws IOException{
	
		// always set the name as first thing
		json.name(attribute.getKey());

		// check if we are dealing with a nested attr
		boolean nested = attribute.hasAttributes();
		if(nested){
			json.beginObject();
			json.name("value");	
		}

		if (attribute instanceof XAttributeTimestamp) {
			Date timestamp = ((XAttributeTimestamp) attribute).getValue();
			json.value(dateFormat.format(timestamp));

		} else if (attribute instanceof XAttributeDiscrete ||  attribute instanceof XAttributeContinuous ) {
			json.value( Double.parseDouble(attribute.toString()));
		} else if (attribute instanceof XAttributeBoolean ) {
			json.value( ((XAttributeBoolean) attribute).getValue());
		} else if (attribute instanceof XAttributeList ) {
			// get XAttributeList
			Collection<XAttribute> list = ((XAttributeList) attribute).getCollection();
			// begin the json array [
			json.beginArray();
			// for every attribute in the list create a json object
			for (XAttribute attr : list) {
				// begin the object {
				json.beginObject();
				// run the method with the attribute
				addAttr(attr,json);
				// end the object }
				json.endObject();
			}
			//end the json array ]
			json.endArray();
		} else if (attribute instanceof XAttributeContainer ) {
			Collection<XAttribute> container = ((XAttributeContainer) attribute).getCollection();
			json.beginObject();
			for (XAttribute attr : container) {
				addAttr(attr,json);
			}
			json.endObject();
		} else {
			json.value(attribute.toString());

		}


		if(nested){
			// add nested attributes
			json.name("nested-attrs");
			// begin nested-attributes object
			json.beginObject();
			for(XAttribute attr : attribute.getAttributes().values()) {
				addAttr(attr,json);
			}
			// end nested-attributes object }
			json.endObject();

			//end the whole object opened in the begining of the method
			json.endObject();
		}

	}




	/**
	 * toString() defaults to getName().
	 */
	public String toString() {
		return this.getName();
	}

}