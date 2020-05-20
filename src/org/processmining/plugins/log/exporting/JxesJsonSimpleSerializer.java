package org.processmining.plugins.log.exporting;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * XES serialization to JXES including all trace/event attributes.
 *
 * @author Hossameldin Khalifa
 *
 */
public final class JxesJsonSimpleSerializer implements XSerializer {
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
		XLogging.log("start serializing log to .json (json simple)", XLogging.Importance.DEBUG);
		long start = System.currentTimeMillis();


		//		PrintStream err = new PrintStream(new java.io.OutputStream(){
		//			public void write(int b) throws IOException {
		//
		//			}});
		//		System.setErr(err);

		//create output json object
		JSONObject output = new JSONObject();

		JSONObject logAttrs = new JSONObject();
		JSONObject logChildren = new JSONObject();

		try {
			
			// add log properties
			logAttrs.put("xes.version", XRuntimeUtils.XES_VERSION);
			logAttrs.put("xes.features", "nested-attributes");
			logAttrs.put("openxes.version", XRuntimeUtils.OPENXES_VERSION);
			//logTag.addAttribute("xmlns", "http://www.xes-standard.org/");

			// iterate over log attributes
			for (XAttribute attr : log.getAttributes().values()) {
				addAttr(attr,logChildren);
			}

			// add log attrs to output
			output.put("log-properties",logAttrs);
			output.put("log-attrs",logChildren);


		}catch (JSONException e) {

		}
		
		//create global attributes
		JSONObject global = new JSONObject();
		JSONObject globalTrace = new JSONObject();
		JSONObject globalEvent = new JSONObject();

		try{

			//iterate over global trace attrs
			for (XAttribute attr : log.getGlobalTraceAttributes()) {
				addAttr(attr,globalTrace);
			}
			//iterate over global event attrs
			for (XAttribute attr : log.getGlobalEventAttributes()) {
				addAttr(attr,globalEvent);
			}

			// add them to output
			global.put("trace", globalTrace);
			global.put("event", globalEvent);
			output.put("global", global);

		}catch (JSONException e) {

		}

		
		JSONArray extensions =  new JSONArray();
		try {
			
			// iterate over all extensions
			for (XExtension extension : log.getExtensions()) {
				JSONObject extensionObject =  new JSONObject();

				extensionObject.put("name", extension.getName());
				extensionObject.put("prefix", extension.getPrefix());
				extensionObject.put("uri", extension.getUri().toString());

				extensions.put(extensionObject);

			}
			// add extensions to output
			output.put("extensions",extensions);

		}catch (JSONException e) {

		}


		JSONObject classifiers = new JSONObject();

		try {
			//iterate over all classifiers 
			for (XEventClassifier classifier : log.getClassifiers()) {
				if (classifier instanceof XEventAttributeClassifier) {
					XEventAttributeClassifier attrClass = (XEventAttributeClassifier) classifier;
					JSONArray classifierArray =  new JSONArray();


					String[] classifierKeys = attrClass.getDefiningAttributeKeys();
					//add classifierKeys to output array
					for (int i = 0; i < classifierKeys.length; i++) {
						classifierArray.put(classifierKeys[i]);
					}
					
					// add classifier to object of classifiers 
					classifiers.put(attrClass.name(), classifierArray );

				}
			}
			// add classifiers object to output
			output.put("classifiers",classifiers);

		}catch (JSONException e) {

		}





		JSONArray traces =  new JSONArray();
		try {
			//iterate over all traces
			for (XTrace trace : log) {
				
				traces.put(compileTrace(trace));

			}
			// add traces array to output
			output.put("traces", traces);
		}catch (JSONException e) {

		}

		// write object to json file
		FileChannel dstChannel = ((FileOutputStream) out).getChannel();
		String out_str = output.toString();
		byte[] output_string = out_str.getBytes("UTF-8");
		out_str = null;
		out.write(output_string);

		out.close();
		String duration = " (" + (System.currentTimeMillis() - start) + " msec.)";
		XLogging.log("finished serializing log" + duration, XLogging.Importance.DEBUG);
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
	}

	private JSONObject compileTrace(XTrace trace) throws JSONException{
		
		JSONObject traceJson = new JSONObject();
		JSONObject attributes = new JSONObject();
		JSONArray events =  new JSONArray();

		try {
			//iterate over all trace attrs
			for (XAttribute attr : trace.getAttributes().values()){
				addAttr(attr,attributes);
			}


		}catch (JSONException e){

		}

		// iterate over all trace events
		for (ListIterator<XEvent> iterator = trace.listIterator(); iterator.hasNext();) {
			XEvent event = iterator.next();
			events.put(compileEvent(event));
		}

		try {
			// add traces attrs to output
			traceJson.put("attrs", attributes);
			// add trace events to output
			traceJson.put("events",events);



		}catch (JSONException e){

		}

		return traceJson;
	}


	private JSONObject compileEvent(XEvent event) throws JSONException{
		JSONObject eventJson = new JSONObject();
		try {
			//iterate over all event attrs
			for (XAttribute attr : event.getAttributes().values()) {
				addAttr(attr,eventJson);
			}
		}catch (JSONException e) {

		}
		return eventJson;
	}



	/**
	 * Helper method, returns the String representation of the attribute
	 *
	 * @param attribute
	 *            The attributes to convert
	 */
	protected String convertAttribute(XAttribute attribute) {
		if (attribute instanceof XAttributeTimestamp) {
			Date timestamp = ((XAttributeTimestamp) attribute).getValue();
			return dateFormat.format(timestamp);
		} else {
			return attribute.toString();
		}
	}


	protected void addAttr(XAttribute attribute,JSONObject json) throws JSONException{
		try {


			if (attribute instanceof XAttributeTimestamp) {
				Date timestamp = ((XAttributeTimestamp) attribute).getValue();
				json.put(attribute.getKey(),timestamp.toString());

			} else if (attribute instanceof XAttributeDiscrete ||  attribute instanceof XAttributeContinuous ) {
				json.put(attribute.getKey(), Double.parseDouble(attribute.toString()));
			} else if (attribute instanceof XAttributeBoolean ) {
				json.put(attribute.getKey(), ((XAttributeBoolean) attribute).getValue());
			} else if (attribute instanceof XAttributeList ) {
				Collection<XAttribute> list = ((XAttributeList) attribute).getCollection();
				JSONArray jsonList = new JSONArray();
				for (XAttribute attr : list) {
					JSONObject jsonObj = new JSONObject();
					addAttr(attr,jsonObj);
					jsonList.put(jsonObj);
				}
				json.put(attribute.getKey(), jsonList);
			} else if (attribute instanceof XAttributeContainer ) {
				Collection<XAttribute> container = ((XAttributeContainer) attribute).getCollection();
				JSONObject jsonObj = new JSONObject();
				for (XAttribute attr : container) {
					addAttr(attr,jsonObj);
				}
				json.put(attribute.getKey(),jsonObj);
			} else {
				json.put(attribute.getKey(), attribute.toString());

			}

			if(attribute.hasAttributes()) {
				JSONObject object =  new JSONObject();
				JSONObject nestedAttributes = new JSONObject();
				for(XAttribute attr : attribute.getAttributes().values()) {
					addAttr(attr,nestedAttributes);
				}
				object.put("nested-attrs", nestedAttributes);
				object.put("value", json.get(attribute.getKey()));
				json.put(attribute.getKey(),object);
			}


		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}




	/**
	 * toString() defaults to getName().
	 */
	public String toString() {
		return this.getName();
	}

}
