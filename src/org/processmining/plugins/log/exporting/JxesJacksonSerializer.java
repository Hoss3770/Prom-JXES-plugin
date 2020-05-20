package org.processmining.plugins.log.exporting;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * XES serialization to JXES including all trace/event attributes.
 *
 * @author Hossameldin Khalifa
 *
 */
public final class JxesJacksonSerializer implements XSerializer {

	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	private final ObjectMapper mapper = new ObjectMapper();

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
		XLogging.log("start serializing log to .json (Jackson)", XLogging.Importance.DEBUG);
		long start = System.currentTimeMillis();


		//		PrintStream err = new PrintStream(new java.io.OutputStream(){
		//			public void write(int b) throws IOException {
		//
		//			}});
		//		System.setErr(err);



		//create output json object
		ObjectNode output = mapper.createObjectNode();;

		ObjectNode logAttrs = mapper.createObjectNode();;
		ObjectNode logChildren = mapper.createObjectNode();;

		// add log properties
		logAttrs.put("xes.version", XRuntimeUtils.XES_VERSION);
		logAttrs.put("xes.features", "nested-attributes");
		logAttrs.put("openxes.version", XRuntimeUtils.OPENXES_VERSION);
		//		logTag.addAttribute("xmlns", "http://www.xes-standard.org/");


		// iterate over log attributes
		for (XAttribute attr : log.getAttributes().values()) {
			addAttr(attr,logChildren);
		}

		// add log attrs to output
		output.set("log-properties",logAttrs);
		output.set("log-attrs",logChildren);



		//create global attributes
		ObjectNode global = mapper.createObjectNode();;
		ObjectNode globalTrace = mapper.createObjectNode();;
		ObjectNode globalEvent = mapper.createObjectNode();;


		//iterate over global trace attrs
		for (XAttribute attr : log.getGlobalTraceAttributes()) {
			addAttr(attr,globalTrace);
		}
		//iterate over global event attrs
		for (XAttribute attr : log.getGlobalEventAttributes()) {
			addAttr(attr,globalEvent);
		}

		// add them to output
		global.set("trace", globalTrace);
		global.set("event", globalEvent);
		output.set("global", global);





		ArrayNode extensions =  mapper.createArrayNode();;
		// iterate over all extensions
		for (XExtension extension : log.getExtensions()) {
			ObjectNode extensionObject =  mapper.createObjectNode();;

			extensionObject.put("name", extension.getName());
			extensionObject.put("prefix", extension.getPrefix());
			extensionObject.put("uri", extension.getUri().toString());

			extensions.add(extensionObject);
		}

		// add extensions to output
		output.set("extensions",extensions);



		ObjectNode classifiers = mapper.createObjectNode();;
		// iterate over all classifiers 
		for (XEventClassifier classifier : log.getClassifiers()) {
			if (classifier instanceof XEventAttributeClassifier) {
				XEventAttributeClassifier attrClass = (XEventAttributeClassifier) classifier;
				ArrayNode classifierArray =  mapper.createArrayNode();;

				
				String[] classifierKeys = attrClass.getDefiningAttributeKeys();
				//add classifierKeys to output array
				for (int i = 0; i < classifierKeys.length; i++) {
					classifierArray.add(classifierKeys[i]);
				}
				// add classifier to object of classifiers 
				classifiers.set(attrClass.name(), classifierArray );

			}
		}
		// add classifiers object to output
		output.set("classifiers",classifiers);


		
		ArrayNode traces =  mapper.createArrayNode();;
		//iterate over all traces
		for (XTrace trace : log) {
			traces.add(compileTrace(trace));

		}
		// add traces array to output
		output.set("traces", traces);

		
		// write object to json file
		mapper.writeValue(out, output);

		out.close();
		String duration = " (" + (System.currentTimeMillis() - start) + " msec.)";
		XLogging.log("finished serializing log" + duration, XLogging.Importance.DEBUG);
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
	}

	private ObjectNode compileTrace(XTrace trace){
		ObjectNode traceJson = mapper.createObjectNode();;
		ObjectNode attributes = mapper.createObjectNode();;
		ArrayNode events =  mapper.createArrayNode();;


		//iterate over all trace attrs
		for (XAttribute attr : trace.getAttributes().values()){
			addAttr(attr,attributes);
		}

		// iterate over all trace events
		for (ListIterator<XEvent> iterator = trace.listIterator(); iterator.hasNext();) {
			XEvent event = iterator.next();
			events.add(compileEvent(event));
		}


		// add traces attrs to output
		traceJson.set("attrs", attributes);
		
		// add trace events to output
		traceJson.set("events",events);


		return traceJson;
	}


	private ObjectNode compileEvent(XEvent event){
		ObjectNode eventJson = mapper.createObjectNode();;
		//iterate over all event attrs
		for (XAttribute attr : event.getAttributes().values()) {
			addAttr(attr,eventJson);
		}
		return eventJson;
	}




	protected void addAttr(XAttribute attribute,ObjectNode json){



		if (attribute instanceof XAttributeTimestamp) {
			Date timestamp = ((XAttributeTimestamp) attribute).getValue();
			json.put(attribute.getKey(), dateFormat.format(timestamp));

		} else if (attribute instanceof XAttributeDiscrete ||  attribute instanceof XAttributeContinuous ) {
			json.put(attribute.getKey(), Double.parseDouble(attribute.toString()));
		} else if (attribute instanceof XAttributeBoolean ) {
			json.put(attribute.getKey(), ((XAttributeBoolean) attribute).getValue());
		} else if (attribute instanceof XAttributeList ) {
			Collection<XAttribute> list = ((XAttributeList) attribute).getCollection();
			ArrayNode jsonList = mapper.createArrayNode();;
			for (XAttribute attr : list) {
				ObjectNode jsonObj = mapper.createObjectNode();;
				addAttr(attr,jsonObj);
				jsonList.add(jsonObj);
			}
			json.set(attribute.getKey(), jsonList);
		} else if (attribute instanceof XAttributeContainer ) {
			Collection<XAttribute> container = ((XAttributeContainer) attribute).getCollection();
			ObjectNode jsonObj = mapper.createObjectNode();;
			for (XAttribute attr : container) {
				addAttr(attr,jsonObj);
			}
			json.set(attribute.getKey(),jsonObj);
		} else {
			json.put(attribute.getKey(), attribute.toString());

		}

		if(attribute.hasAttributes()) {
			ObjectNode object =  mapper.createObjectNode();;
			ObjectNode nestedAttributes = mapper.createObjectNode();;
			for(XAttribute attr : attribute.getAttributes().values()) {
				addAttr(attr,nestedAttributes);
			}
			object.set("nested-attrs", nestedAttributes);
			object.set("value", json.get(attribute.getKey()));
			json.set(attribute.getKey(),object);
		}

	}




	/**
	 * toString() defaults to getName().
	 */
	public String toString() {
		return this.getName();
	}

}
