package org.processmining.plugins.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.time.FastDateFormat;
import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeBooleanImpl;
import org.deckfour.xes.model.impl.XAttributeContainerImpl;
import org.deckfour.xes.model.impl.XAttributeContinuousImpl;
import org.deckfour.xes.model.impl.XAttributeDiscreteImpl;
import org.deckfour.xes.model.impl.XAttributeListImpl;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.deckfour.xes.model.impl.XAttributeMapImpl;
import org.deckfour.xes.model.impl.XAttributeTimestampImpl;
import org.deckfour.xes.model.impl.XEventImpl;
import org.deckfour.xes.model.impl.XTraceImpl;
import org.processmining.contexts.uitopia.annotations.UIImportPlugin;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginLevel;
import org.processmining.log.utils.XLogBuilder;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.EncodingMode;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.DecodingMode;

@Plugin(name = "Open JXES (Jsoninter iter)", level = PluginLevel.PeerReviewed, parameterLabels = {
		"Filename" }, returnLabels = { "Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (Jsoninter iter)", extensions = { "json" })
public class OpenNaiveLogFilePluginJsoninter_iter extends OpenLogFilePlugin {
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	// create a XLogBuilder to iteratively build the XLog object
	XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");

	// return the XLog object from the builder
	XLog log = builder.build();
	
	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
			throws Exception {

		
		//		String attr = iter.readObject();
		//		String val = iter.readString();
		//		System.out.println("attr: " + attr);
		//		System.out.println("val: " + val);

		// set the name displayed in prom to the name of the file
		context.getFutureResult(0).setLabel(filename);

		// set dynamic mode for performance reasons --> see https://jsoniter.com/java-features.html
		JsonIterator.setMode(DecodingMode.DYNAMIC_MODE_AND_MATCH_FIELD_WITH_HASH);
		JsonStream.setMode(EncodingMode.DYNAMIC_MODE);
		
		long start = System.currentTimeMillis();
		System.out.println(fileSizeInBytes);
		System.out.println(((int) fileSizeInBytes));
		int i = 0;
		BufferedReader bR = new BufferedReader(  new InputStreamReader(input));
		String line = "";

		StringBuilder responseStrBuilder = new StringBuilder();
		int count = 0;
		int intch;
	    while (((intch = bR.read()) != -1) && count < fileSizeInBytes*8) {
	    	responseStrBuilder.append((char) intch);
	    	count++;
	    }
	    
		
	    input.close();

		String duration = " parsing input (" + (System.currentTimeMillis() - start) + " msec.)";
		System.out.println(duration);
		
		JsonIterator iter = JsonIterator.parse(responseStrBuilder.toString());
		//		PrintStream err = new PrintStream(new java.io.OutputStream(){
		//			public void write(int b) throws IOException {
		//				
		//			}});
		//		System.setErr(err);
		////		System.setOut(err);

		//TODO: check if order matters 

		// iterate over the whole json file
		for (String key = iter.readObject(); key != null; key = iter.readObject()) {
			switch (key) {
				case "log-children" :
					buildLogAttrs(iter);
					break;
				case "extensions" :
					buildExtensions(iter);
					break;
				case "classifiers" :
					buildClassifiers(iter);
					break;
				case "global" :
					buildGlobalAttrs(iter);
					break;
				case "traces" :
					buildTraces(iter);
					break;
				default :
					iter.skip();
					
			}
		}	

		input.close();
		System.out.println(
				"Memory used: " + ((double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
						/ (double) (1024 * 1024)));
		return log;

	}

	/**
	 * 
	 * Depending on the type of the attribute given as a parameter the right
	 * attribute implementation is used.
	 * 
	 * @param the
	 *            attribute name
	 * @param the
	 *            attribute value
	 * @return an attribute ready to be added to the log object
	 * @see org.deckfour.xes.model.XAttribute
	 */

	XAttribute createAttr(String key, Object attr) {
		XAttribute attribute = null;

		if (attr instanceof String) {
			String text = attr.toString();
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key, date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl(key, text);
			}
		} else if (attr instanceof Integer) {
			long number = new Long( (int) attr);
			attribute = new XAttributeDiscreteImpl(key, number);
		} else if (attr instanceof Double) {
			attribute = new XAttributeContinuousImpl(key, (double) attr);
		} else if (attr instanceof ArrayList) {
			ArrayList arr = (ArrayList) attr;
			XAttributeListImpl list = new XAttributeListImpl(key);
			for (Object arrElement : arr) {
				// get the object inside the array element [{"key":"value"}]
				Map<String, Object> hashMap = (HashMap) arrElement;
				Entry<String, Object> entry = hashMap.entrySet().iterator().next();
				// use key value pairs to create attributes and add them to the collection
				list.addToCollection(createAttr(entry.getKey(), entry.getValue()));
			}
			attribute = list;
		} else if (attr instanceof HashMap) {
			Map<String, Object> hashMap = (HashMap) attr;
			Iterator<Entry<String, Object>> iterator = hashMap.entrySet().iterator();

			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttribute map = new XAttributeContainerImpl(key);

			// the case of nested attributes
			if (hashMap.size() == 2 && hashMap.get("value") != null && hashMap.get("nested-attributes") != null) {
				map = createAttr(key, hashMap.get("value"));
				hashMap = (HashMap) hashMap.get("nested-attributes");
			}

			while (iterator.hasNext()) {
				Entry<String, Object> element = iterator.next();
				String elementKey = element.getKey();
				values.put(elementKey, createAttr(elementKey, element.getValue()));
			}

			map.setAttributes(values);
			attribute = map;
		} else if (attr instanceof Boolean) {
			attribute = new XAttributeBooleanImpl(key, (boolean) attr);
		}
		return attribute;
	}

	void buildLogAttrs(JsonIterator iter) throws IOException {
		//create a map which will hold all log attributes 
		XAttributeMapImpl logAttributes = new XAttributeMapImpl();

		// for each attribute --> create attribute --> add it to the map
		Object value = null;
		for (String key = iter.readObject(); key != null; key = iter.readObject()) {
			value = iter.read();
			logAttributes.put(key, createAttr(key, value));
		}

		//		logChildren.forEach((key, value) -> logAttributes.put(key,createAttr(key, value)));
		// set log attributes to the map.
		log.setAttributes(logAttributes);

	}

	// add extensions to the XLog object
	void buildExtensions(JsonIterator iter) throws IOException {
		// read the begining of the array [ 
		// and iterate over the array elements = over all extensions

		String prefix = null;

		while (iter.readArray()) {
			for (String key = iter.readObject(); key != null; key = iter.readObject()) {
				if (key.equals("prefix")) {
					prefix = iter.readString();

				} else {
					iter.skip();
				}
			}

			//use the prefix of the extension and the extension-manger to check if the extension is known
			XExtension extension = XExtensionManager.instance().getByPrefix(prefix);
			if (extension != null) {
				//extension known
				// add to the list of extensions of the XLog
				log.getExtensions().add(extension);
			} else {
				//extension was not found --> unknown
				// throw warning and skip extension 
				System.err.println("Unknown extension: " + prefix);
			}
		}

	}

	// add classifiers to the XLog object
	void buildClassifiers(JsonIterator iter) throws IOException {
		//iterate over all classifiers

		for (String classifierName = iter.readObject(); classifierName != null; classifierName = iter.readObject()) {
			//String array of classifier-keys
			Object array = iter.read();
			ArrayList classifierKeys = ((ArrayList) array);
			String[] classifierKeysArray = new String[classifierKeys.size()];
			for (int i = 0; i < classifierKeys.size(); i++) {
				classifierKeysArray[i] = (String) classifierKeys.get(i);
			}
			log.getClassifiers().add(new XEventAttributeClassifier(classifierName, classifierKeysArray));
		}

	}

	// add global attributes
	void buildGlobalAttrs(JsonIterator iter) throws IOException {

		Object value = null;
		for (String scope = iter.readObject(); scope != null; scope = iter.readObject()) {
			for (String key = iter.readObject(); key != null; key = iter.readObject()) {
				value = iter.read();
				if (scope.equals("trace")) {
					log.getGlobalTraceAttributes().add(createAttr(key, value));
				} else {
					log.getGlobalEventAttributes().add(createAttr(key, value));
				}
			}

		}

	}
	
	//build all traces
	void buildTraces(JsonIterator iter) throws IOException {
		
		Object value = null;
		//used to name traces
		int i = -1;
		
		// used to name events
		int j = -1;
		//loop on traces
		XAttributeMapImpl traceAttrs = new XAttributeMapImpl();
		XTrace trace = null;
		while(iter.readArray()) {
			i += 1;
			for (String key = iter.readObject(); key != null; key = iter.readObject()) {
				if (key.equals("attrs")){
					for (String attrKey = iter.readObject(); attrKey != null; attrKey = iter.readObject()) {
						value = iter.read();
						traceAttrs.put(attrKey,createAttr(attrKey, value));
					}
					trace = new XTraceImpl(traceAttrs);
				}else {
					//events
					
					
					j = -1;
					j+=1;
					while(iter.readArray()) {
						XEvent event = new XEventImpl();
						XAttributeMapImpl eventAttrs = new XAttributeMapImpl();
						for (String attrKey = iter.readObject(); attrKey != null; attrKey = iter.readObject()) {
							value = iter.read();
							eventAttrs.put(attrKey,createAttr(attrKey, value));
						}
						event.setAttributes(eventAttrs);
						trace.add(event);
					}
					
				}
			}
			log.add(trace);
		}

	}

	//	
	//XAttribute createAttr(String key, Any attr) {
	//		Object attrAsObject = attr.object();
	//		XAttribute attribute = null;
	//		if (attrAsObject instanceof Boolean) {
	//			attribute = new XAttributeBooleanImpl(key, attr.toBoolean());
	//		} else if (attrAsObject instanceof Double) {
	//			attribute = new XAttributeContinuousImpl(key, (Double) attrAsObject );
	//		}else if (attrAsObject instanceof Long) {	
	//				attribute = new XAttributeDiscreteImpl(key, (Long) attrAsObject);
	//		} else if (attrAsObject instanceof ArrayList) {
	//			XAttributeListImpl list = new XAttributeListImpl(key);
	//			for(Any arrElement :  attr){
	//				String elementKey = (String) arrElement.keys().iterator().next();
	//				XAttribute arrAttribute = createAttr(elementKey, arrElement.get(elementKey));
	//				list.addToCollection(arrAttribute);	
	//			}
	//			attribute = list;
	//		} else if (attrAsObject instanceof HashMap) {
	//			
	//			Set<String> objectKeys = attr.keys();
	//			XAttributeMapImpl values = new XAttributeMapImpl();
	//			XAttributeContainerImpl map = new XAttributeContainerImpl(key);
	//			
	//			// the case of nested attributes
	//			if (attr.size() == 2 && attr.get("value").valueType() != ValueType.INVALID && attr.get("nested-attributes").valueType() != ValueType.INVALID) {
	//				return createNestedAttribute(key, attr);
	//			}
	//
	//			objectKeys = attr.keys();
	//
	//			for(String objecgtKey : objectKeys){
	//				XAttribute objectAttribute = createAttr(objecgtKey, attr.get(objecgtKey));
	//				values.put(objecgtKey, objectAttribute);
	//			}
	//			map.setAttributes(values);
	//			attribute = map;
	//		} else if (attrAsObject instanceof String) {
	//			String text = attr.toString();
	//			Date date;
	//			try {
	//				date = DateUtil.parse(text);
	//				attribute = new XAttributeTimestampImpl(key, date);
	//			} catch (ParseException e) {
	//				attribute = new XAttributeLiteralImpl(key, text);
	//			}
	//
	//		}
	//		return attribute;
	//	}

	//	XAttribute createNestedAttribute(String key, Any attr){
	//		XAttribute attribute = createAttr(key, attr.get("value"));
	//
	//		Map<String, Any> nestedAttributes = attr.get("nested-attributes");
	//		XAttributeMapImpl values = new XAttributeMapImpl();
	//		
	//		Set<String> objectKeys = nestedAttributes.keys();
	//		for(String objectKey : objectKeys){
	//			XAttribute objectAttribute = createAttr(objectKey, nestedAttributes.get(objectKey));
	//			values.put(objectKey, objectAttribute);
	//		}
	//
	//		attribute.setAttributes(values);
	//		return attribute;
	//	}

}
