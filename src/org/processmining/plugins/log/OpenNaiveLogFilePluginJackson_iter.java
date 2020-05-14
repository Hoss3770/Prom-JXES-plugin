package org.processmining.plugins.log;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.lang3.time.FastDateFormat;
import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.extension.std.XConceptExtension;
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

@Plugin(name = "Open JXES (Jackson iter)", level = PluginLevel.PeerReviewed, parameterLabels = {
		"Filename" }, returnLabels = { "Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (Jackson iter)", extensions = { "json" })
public class OpenNaiveLogFilePluginJackson_iter extends OpenLogFilePlugin {
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	// create a XLogBuilder to iteratively build the XLog object
	XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");

	// return the XLog object from the builder
	XLog log = builder.build();
	XConceptExtension conceptInstance = XConceptExtension.instance();
	ObjectMapper objectMapper = new ObjectMapper();

	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
			throws Exception {
		 
		// set the name displayed in prom to name of the file
		context.getFutureResult(0).setLabel(filename);
	

		JsonFactory jsonFactory = new JsonFactory();
		JsonParser jsonParser = jsonFactory.createParser(input);


		//TODO: check if order matters 

		// iterate over the whole json file

		// enter object {
		jsonParser.nextToken();
		while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
			String key = jsonParser.getCurrentName();
			switch (key) {
				case "log-attrs" :
					//skip log attrs
					while(jsonParser.nextToken() != JsonToken.END_OBJECT);
					break;
				case "log-children" :
					buildLogAttrs(jsonParser);
					break;
				case "extensions" :
					buildExtensions(jsonParser);
					break;
				case "classifiers" :
					buildClassifiers(jsonParser);
					break;
				case "global" :
					buildGlobalAttrs(jsonParser);
					break;
				case "traces" :
					buildTraces(jsonParser);
					break;
				default :
					jsonParser.nextToken();

			}
		}

		jsonParser.close();
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
	 * @throws IOException
	 * @see org.deckfour.xes.model.XAttribute
	 */

	XAttribute createAttr(String key, JsonParser iter) throws IOException {
		XAttribute attribute = null;
		JsonToken val = iter.nextValue();
		if (val == JsonToken.VALUE_STRING) {
			String text = iter.getText();
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key, date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl(key, text);
			}
		} else if (val == JsonToken.VALUE_NUMBER_INT) {
			attribute = new XAttributeDiscreteImpl(key, iter.getIntValue());
		} else if (val == JsonToken.VALUE_NUMBER_FLOAT) {
			attribute = new XAttributeContinuousImpl(key, iter.getDoubleValue());
		} else if (val == JsonToken.START_ARRAY) {

			XAttributeListImpl list = new XAttributeListImpl(key);
			String elementKey = null;

			while (iter.nextToken() != JsonToken.END_ARRAY) {
				//move to the object start {
				iter.nextToken();
				//move to the first (and only) element in object 
				iter.nextToken();
				elementKey = iter.getCurrentName();
				list.addToCollection(createAttr(elementKey, iter));
			}
			attribute = list;
		} else if (val == JsonToken.START_OBJECT) {

			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttribute map = new XAttributeContainerImpl(key);

			String elementKey = null;

			boolean nested = false;
			while (iter.nextToken() != JsonToken.END_OBJECT) {
				elementKey = iter.getCurrentName();

				// to manage the case of nested attributes
				if (elementKey.equals("value")) {
					map = createAttr(key, iter);
				}
				values.put(elementKey, createAttr(elementKey, iter));

			}

			map.setAttributes(values);
			attribute = map;
		} else if ((val == JsonToken.VALUE_TRUE) || (val == JsonToken.VALUE_FALSE)) {
			attribute = new XAttributeBooleanImpl(key, iter.getBooleanValue());
		}

		return attribute;
	}

	void buildLogAttrs(JsonParser iter) throws IOException {
		//move to the first attribute in the log-children object.
		iter.nextToken();

		//create a map which will hold all log attributes 
		XAttributeMapImpl logAttributes = new XAttributeMapImpl();

		// for each attribute --> create attribute --> add it to the map
		Object value = null;
		String key = null;
		while (iter.nextToken() != JsonToken.END_OBJECT) {
			key = iter.getCurrentName();
			logAttributes.put(key, createAttr(key, iter));
		}

		//		logChildren.forEach((key, value) -> logAttributes.put(key,createAttr(key, value)));
		// set log attributes to the map.
		log.setAttributes(logAttributes);

	}

	// add extensions to the XLog object
	void buildExtensions(JsonParser iter) throws IOException {
		// and iterate over the array elements = over all extensions
		String prefix = null;
		// start array
		iter.nextToken();
		// enter object { with every iteration 
		while (iter.nextToken() != JsonToken.END_ARRAY) {
			while (true) {
				JsonToken token = iter.nextToken();
				if(token == JsonToken.END_OBJECT) break;
				String key = iter.getCurrentName();
				iter.nextValue();
				if (key.equals("prefix")) {
					prefix = iter.getText();
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
	void buildClassifiers(JsonParser iter) throws IOException {
		//move to start of object
		iter.nextToken();

		//iterate over all classifiers
		String classifierName = null;
		while (iter.nextToken() != JsonToken.END_OBJECT) {
			//String array of classifier-keys
			classifierName = iter.getCurrentName();
			//move to start of array
			iter.nextToken();
			ArrayList<String> classifierKeys = new ArrayList<String>();
			while (iter.nextToken() != JsonToken.END_ARRAY) {
				classifierKeys.add(iter.getText());
			}

			String[] classifierKeysArray = new String[classifierKeys.size()];
			for (int i = 0; i < classifierKeys.size(); i++) {
				classifierKeysArray[i] = classifierKeys.get(i);
			}
			log.getClassifiers().add(new XEventAttributeClassifier(classifierName, classifierKeysArray));
		}

	}


	// add global attributes
	void buildGlobalAttrs(JsonParser iter) throws IOException {
		//move to the start of the object
		iter.nextToken();
		Object value = null;
		String scope = null;
		String key = null;
		//iterate over all value-key pairs
		while (iter.nextToken() != JsonToken.END_OBJECT) {
			//get the name of the scope (trace or event)
			scope = iter.getCurrentName();
			iter.nextToken();
			//iterate over key value pairs in current scope
			while (iter.nextToken() != JsonToken.END_OBJECT) {
				key = iter.getCurrentName();
				if (scope.equals("trace")) {
					log.getGlobalTraceAttributes().add(createAttr(key, iter));
				} else {
					log.getGlobalEventAttributes().add(createAttr(key, iter));
				}
			}

		}

	}

	//	
	//build all traces
	void buildTraces(JsonParser iter) throws IOException {
		// start array 
		iter.nextToken();

		//used to name traces
		int i = -1;

		// used to name events
		int j = -1;
		//loop on traces
		
		XAttributeMapImpl traceAttrs = new XAttributeMapImpl();
		//iterate over traces array
		while (iter.nextToken() != JsonToken.END_ARRAY) {
			XTrace trace = null;
			i += 1;
			while (iter.nextToken() != JsonToken.END_OBJECT) {
				String key = iter.getCurrentName();
				if (key.equals("attrs")) {
					//  move to beginning of attrs object
					iter.nextToken();
					// iterate over all trace attributes
					while (iter.nextToken() != JsonToken.END_OBJECT) {
						String attrKey = iter.getCurrentName();
						traceAttrs.put(attrKey, createAttr(attrKey, iter));
					}
					trace = new XTraceImpl(traceAttrs);
				} else {
					//events
					j = -1;
					j += 1;
					// move to beginning of event object {
					iter.nextToken();
					//iterate over all event attrs
					while (iter.nextToken() != JsonToken.END_ARRAY) {

						XEvent event = new XEventImpl();
						XAttributeMapImpl eventAttrs = new XAttributeMapImpl();
						while (iter.nextToken() != JsonToken.END_OBJECT) {
							String attrKey = iter.getCurrentName();
							eventAttrs.put(attrKey, createAttr(attrKey, iter));
						}
						
						// set the attrs of the current event
						event.setAttributes(eventAttrs);
						// assign name to event
						conceptInstance.assignName(event, "" + j);
						//add event to trace
						trace.add(event);
					}

				}
			}
			//assign name to trace
			conceptInstance.assignName(trace, "" + i);
			//add trace to log
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
