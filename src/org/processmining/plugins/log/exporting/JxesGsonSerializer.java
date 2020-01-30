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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * XES serialization to JXES including all t
import com.google.gson.JsonObject;race/event attributes.
 *
 * @author Hossameldin Khalifa
 *
 */
public final class JxesGsonSerializer implements XSerializer {

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


//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//
//			}});
//		System.addErr(err);
		
		
		
		
		JsonObject output =new JsonObject();

		JsonObject logAttrs =new JsonObject();
		JsonObject logChildren =new JsonObject();


		logAttrs.addProperty("xes.version", XRuntimeUtils.XES_VERSION);
		logAttrs.addProperty("xes.features", "nested-attributes");
		logAttrs.addProperty("openxes.version", XRuntimeUtils.OPENXES_VERSION);
//		logTag.addAttribute("xmlns", "http://www.xes-standard.org/");


		for (XAttribute attr : log.getAttributes().values()) {
			addAttr(attr,logChildren);
		}


		output.add("log-attrs",logAttrs);
		output.add("log-children",logChildren);



		JsonObject global =new JsonObject();
		JsonObject globalTrace =new JsonObject();
		JsonObject globalEvent =new JsonObject();



		for (XAttribute attr : log.getGlobalTraceAttributes()) {
			addAttr(attr,globalTrace);
		}
		for (XAttribute attr : log.getGlobalEventAttributes()) {
			addAttr(attr,globalEvent);
		}


		global.add("trace", globalTrace);
		global.add("event", globalEvent);
		output.add("global", global);





		JsonArray extensions =  new JsonArray();

			for (XExtension extension : log.getExtensions()) {
				JsonObject extensionObject = new JsonObject();

				extensionObject.addProperty("name", extension.getName());
				extensionObject.addProperty("prefix", extension.getPrefix());
				extensionObject.addProperty("uri", extension.getUri().toString());

				extensions.add(extensionObject);
			}

			output.add("extensions",extensions);



		JsonObject classifiers =new JsonObject();

			for (XEventClassifier classifier : log.getClassifiers()) {
				if (classifier instanceof XEventAttributeClassifier) {
					XEventAttributeClassifier attrClass = (XEventAttributeClassifier) classifier;
					JsonArray classifierArray =  new JsonArray();

					String[] myArray = attrClass.getDefiningAttributeKeys();
					for (int i = 0; i < myArray.length; i++) {
						classifierArray.add( new JsonPrimitive(myArray[i]));
				      }

					classifiers.add(attrClass.name(), classifierArray );

				}
			}
			output.add("classifiers",classifiers);







		JsonArray traces =  new JsonArray();

			for (XTrace trace : log) {

				traces.add(compileTrace(trace));

			}
			output.add("traces", traces);


//		FileChannel dstChannel = ((FileOutputStream) out).getChannel();
		String out_str = output.toString();
		byte[] output_string = out_str.getBytes("UTF-8");
		out.write(output_string);
//		ByteBuffer buf = ByteBuffer.allocateDirect(output_string.length);
//		for (int k = 0; k< output_string.length; k++){
//			buf.addProperty(output_string[k]);
//			}
//		dstChannel.write(buf);
//		dstChannel.close();

		out.close();
		String duration = " (" + (System.currentTimeMillis() - start) + " msec.)";
		XLogging.log("finished serializing log" + duration, XLogging.Importance.DEBUG);
	}

	private JsonObject compileTrace(XTrace trace){
		JsonObject traceJson =new JsonObject();
		JsonObject attributes =new JsonObject();
		JsonArray events =  new JsonArray();
	

		for (XAttribute attr : trace.getAttributes().values()){
				addAttr(attr,attributes);
		}


		for (ListIterator<XEvent> iterator = trace.listIterator(); iterator.hasNext();) {
			XEvent event = iterator.next();
				events.add(compileEvent(event));
		}



		traceJson.add("attrs", attributes);
		traceJson.add("events",events);


		return traceJson;
	}


	private JsonObject compileEvent(XEvent event){
		JsonObject eventJson =new JsonObject();

		for (XAttribute attr : event.getAttributes().values()) {
			addAttr(attr,eventJson);
		}
		return eventJson;
	}




	protected void addAttr(XAttribute attribute,JsonObject json){
	


		if (attribute instanceof XAttributeTimestamp) {
			Date timestamp = ((XAttributeTimestamp) attribute).getValue();
			json.addProperty(attribute.getKey(), dateFormat.format(timestamp));

		} else if (attribute instanceof XAttributeDiscrete ||  attribute instanceof XAttributeContinuous ) {
			json.addProperty(attribute.getKey(), Double.parseDouble(attribute.toString()));
		} else if (attribute instanceof XAttributeBoolean ) {
			json.addProperty(attribute.getKey(), ((XAttributeBoolean) attribute).getValue());
		} else if (attribute instanceof XAttributeList ) {
			Collection<XAttribute> list = ((XAttributeList) attribute).getCollection();
			JsonArray jsonList = new JsonArray();
			for (XAttribute attr : list) {
				JsonObject jsonObj =new JsonObject();
				addAttr(attr,jsonObj);
				jsonList.add(jsonObj);
			}
			json.add(attribute.getKey(), jsonList);
		} else if (attribute instanceof XAttributeContainer ) {
			Collection<XAttribute> container = ((XAttributeContainer) attribute).getCollection();
			JsonObject jsonObj =new JsonObject();
			for (XAttribute attr : container) {
				addAttr(attr,jsonObj);
			}
			json.add(attribute.getKey(),jsonObj);
		} else {
			json.addProperty(attribute.getKey(), attribute.toString());

		}

		if(attribute.hasAttributes()) {
			JsonObject object = new JsonObject();
			JsonObject nestedAttributes =new JsonObject();
			for(XAttribute attr : attribute.getAttributes().values()) {
				addAttr(attr,nestedAttributes);
			}
			object.add("nested-attributes", nestedAttributes);
			object.add("value", json.get(attribute.getKey()));
			json.add(attribute.getKey(),object);
		}

	}




	/**
	 * toString() defaults to getName().
	 */
	public String toString() {
		return this.getName();
	}

}