package org.processmining.plugins.log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.time.FastDateFormat;
import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.impl.XAttributeBooleanImpl;
import org.deckfour.xes.model.impl.XAttributeContainerImpl;
import org.deckfour.xes.model.impl.XAttributeContinuousImpl;
import org.deckfour.xes.model.impl.XAttributeDiscreteImpl;
import org.deckfour.xes.model.impl.XAttributeListImpl;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.deckfour.xes.model.impl.XAttributeMapImpl;
import org.deckfour.xes.model.impl.XAttributeTimestampImpl;
import org.processmining.contexts.uitopia.annotations.UIImportPlugin;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginLevel;
import org.processmining.log.utils.XLogBuilder;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.DecodingMode;

@Plugin(name = "Open JXES (Jsoninter 1)", level = PluginLevel.PeerReviewed, parameterLabels = { "Filename" }, returnLabels = {
		"Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (Jsoninter 1)", extensions = { "json" })
public class OpenNaiveLogFilePluginJsoninter1 extends OpenLogFilePlugin {
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");

	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
			throws Exception {
		
		// set the name displayed in prom to the name of the file
		context.getFutureResult(0).setLabel(filename);
		
		// set dynamic mode for performance reasons --> see https://jsoniter.com/java-features.html
		JsonIterator.setMode(DecodingMode.DYNAMIC_MODE_AND_MATCH_FIELD_WITH_HASH);
		
		//read json file
		BufferedReader bR = new BufferedReader(  new InputStreamReader(input));
		String line = "";

		StringBuilder responseStrBuilder = new StringBuilder();
		while((line =  bR.readLine()) != null){

		    responseStrBuilder.append(line);
		}
		input.close();
		// parse json string
		Any obj = JsonIterator.deserialize( responseStrBuilder.toString());

		

		//		PrintStream err = new PrintStream(new java.io.OutputStream(){
		//			public void write(int b) throws IOException {
		//				
		//			}});
		//		System.setErr(err);
		////		System.setOut(err);


		// create a XLogBuilder to iteratively build the XLog object
		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");

		//build all traces
		Any traces = obj.get("traces");
		int i = -1;
		//loop on traces
		for (Any trace : traces) {
			i += 1;
			//get map of trace attributes <key,value>
			Map<String, Any> traceAttrs = trace.get("attrs").asMap();
			// add trace to the log with an identifier 
			builder.addTrace("t" + i);
			//for each trace attribute --> create attribute then add it to the trace object
			traceAttrs.forEach((key, value) -> builder.addAttribute(createAttr(key, value)));

			
			// loop on events of the trace 
			Any events = trace.get("events");
			int j = -1;
			for (Any event : events) {
				// for every event get the attributes as a map
				Map<String, Any> eventMap = event.asMap();
				j += 1;
				//add event to trace
				builder.addEvent("e" + j);
				// for every attribute --> create it --> add it to the event object created
				eventMap.forEach((key, value) -> builder.addAttribute(createAttr(key, value)));
			}
		}

		
		// return the XLog object from the builder
		XLog log = builder.build();
		
		//add log-attributes to the XLog object
		// create a map which holds all log attributes <key,value>
		Map<String, Any> logChildren = obj.get("log-children").asMap();
		//create a map which will hold all log attributes 
		XAttributeMapImpl logAttributes = new XAttributeMapImpl();
		// for each attribute --> create attribute --> add it to the map
		logChildren.forEach((key, value) -> logAttributes.put(key,createAttr(key, value)));
		// set log attributes to the map.
		log.setAttributes(logAttributes);

		
		
		
		

		//		System.err.println("log attributes not deinfed correctly in file : skiping");

		// add extensions to the XLog object
		Any jsonExtensions = obj.get("extensions");
		if(jsonExtensions.valueType() != ValueType.NULL) {
			//iterate over all extension 
			for (Any jsonExtension : jsonExtensions) {
	
				XExtension extension = null;
				//use the prefix of the extension and the extension-manger to check if the extension is known
				String prefix = jsonExtension.get("prefix").toString();
				extension = XExtensionManager.instance().getByPrefix(prefix);
	
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
		}else {
			// if the extensions array does not exist or is not defined correctly throw warning and skip parsing the extension
			System.err.println("extensions not deinfed correctly in file : skiping");
		}

		// add classifiers to the XLog object
		//get all classifier names and values
		// as <classifier-name,String array of classifier-keys>
		Map<String, Any> classifiers = obj.get("classifiers").asMap();
		if(classifiers.size() != 0) {
			// get the classifier keys array --> turn it into String[] --> create new classifier attribute --> add it to classifiers list
			classifiers.forEach((key, value) -> log.getClassifiers().add(new XEventAttributeClassifier(key, value.as(String[].class))));
		}else{
			// if the classifiers object does not exist or is not defined correctly throw warning and skip parsing the classifiers
			System.err.println("classfiers not deinfed correctly in file : skiping");
		}

		// add global attributes

		// get global attributes with scope="trace"
		Map<String, Any> globalTrace = obj.get("global","trace").asMap();
		// for each global trace attributes --> create attribute and add it to globalTraceAttributes List.
		globalTrace.forEach((key, value) -> log.getGlobalTraceAttributes().add(createAttr(key, value)));

		// get global attributes with scope="event"
		Map<String, Any> globalEvent = obj.get("global","event").asMap();
		// for each global event attributes --> create attribute and add it to globalEventAttributes List.
		globalEvent.forEach((key, value) -> log.getGlobalEventAttributes().add(createAttr(key, value)));
		
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
		return log;
		
	}
	
	
	/**
	 * 
	 * Depending on the type of the attribute given as a parameter the right attribute implementation is used.
	 * 
	 * @param the attribute name
	 * @param the attribute value	 
	 * @return an attribute ready to be added to the log object
	 * @see org.deckfour.xes.model.XAttribute
	 */
	

	XAttribute createAttr(String key, Any attr) {
		
		XAttribute attribute = null;
		
		if (attr.valueType() == ValueType.STRING) {			
			String text = attr.toString();
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key, date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl(key, text);
			}
		}else if (attr.valueType() == ValueType.NUMBER) {
			double number = attr.toDouble();
			if (number % 1 == 0) {
				attribute = new XAttributeDiscreteImpl(key, (long) number);
			} else {
				attribute = new XAttributeContinuousImpl(key, number);
			}
		} else if (attr.valueType() == ValueType.ARRAY) {

			XAttributeListImpl list = new XAttributeListImpl(key);
			for(Any arrElement :  attr){	
				//get key value pairs
				Map<String, Any> element = arrElement.asMap();
				// use key value pairs to create attributes and add them to the collection
				element.forEach((elementKey, value) -> list.addToCollection(createAttr(elementKey,value)));		
			}
			attribute = list;
		} else if (attr.valueType() == ValueType.OBJECT) {
			Map<String, Any> jsonObject = attr.asMap();
			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttribute map = new XAttributeContainerImpl(key);
			
			// the case of nested attributes
			if (jsonObject.size() == 2 && attr.get("value").valueType() != ValueType.INVALID && attr.get("nested-attributes").valueType() != ValueType.INVALID) {
				map = createAttr(key,attr.get("value"));
				jsonObject = attr.get("nested-attributes").asMap();
			}
			
			jsonObject.forEach((objectKey, value) -> values.put(objectKey,createAttr(objectKey, value)));
			map.setAttributes(values);
			attribute = map;
		} else if (attr.valueType() == ValueType.BOOLEAN) {
			attribute = new XAttributeBooleanImpl(key, attr.toBoolean());
		}
		return attribute;
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
