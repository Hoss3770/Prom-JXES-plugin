package org.processmining.plugins.log;

import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;

import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.classification.XEventClassifier;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper; 

@Plugin(name = "Open JXES (Jackson)", level = PluginLevel.PeerReviewed, parameterLabels = { "Filename" }, returnLabels = { "Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (Jackson)", extensions = {"json"})
public class OpenNaiveLogFilePluginJackson extends OpenLogFilePlugin {
//	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
	throws Exception {
		
		
		// set the name displayed in prom to the name of the file
		context.getFutureResult(0).setLabel(filename);
		
//		StringWriter writer = new StringWriter();
//		IOUtils.copy(input, writer);
//		String theString = writer.toString();
		
		
		
		//read json file
		ObjectMapper mapper = new ObjectMapper();
		JsonNode obj = mapper.readTree(input);
	

		
//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//				
//			}});
//		System.setErr(err);
//		System.setOut(err);
		
		
		
		// create a XLogBuilder to iteratively build the XLog object
		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");
		
			
		//build all traces
		JsonNode traces = obj.get("traces");
		//loop on traces
		for(int i = 0; i < traces.size();i++){
			//get trace as JsonNode
			JsonNode trace =  traces.get(i);
			// get the attributes of this trace
			JsonNode traceAttrs = trace.get("attrs");
			// get the events of this trace
			JsonNode events = trace.get("events");
			// add the trace to the log object
			builder.addTrace("t" + i);
			//loop on the trace attributes
			Iterator<String> traceAttrKeys = traceAttrs.fieldNames();
			while(traceAttrKeys.hasNext()) {
				String key = traceAttrKeys.next();
				// add the attribute to the trace in the log object
				builder.addAttribute(createAttr(key,traceAttrs.get(key)));
			}
			// loop on the events of the trace 
			for(int j = 0; j < events.size();j++){
				builder.addEvent("e" + j);
				JsonNode event =  events.get(j);
				// loop on the event attributes
				Iterator<String> eventAttrKeys = event.fieldNames();
				while(eventAttrKeys.hasNext()) {
					String key = eventAttrKeys.next();
					builder.addAttribute(createAttr(key,event.get(key)));
				}
				
			}
		}
		
		// return the XLog object from the builder
		XLog log =  builder.build();
		
	
		//add log-attributes to the XLog object
		JsonNode logChildren = obj.get("log-children");
		Iterator<String> logKeys = logChildren.fieldNames();
		 
		XAttributeMapImpl logAttributes = new XAttributeMapImpl();
		while(logKeys.hasNext()) {
			String key = logKeys.next();
			logAttributes.put(key, createAttr(key,logChildren.get(key)));
		}
			
		log.setAttributes(logAttributes);
		
		
		// add extensions to the XLog object
		JsonNode jsonExtensions = obj.get("extensions");
		if(jsonExtensions != null ) {
			for(int j = 0; j < jsonExtensions.size();j++){
				XExtension extension = null;
				
				JsonNode jsonExtension = jsonExtensions.get(j);
				
				String prefix = jsonExtension.get("prefix").asText();
				extension = XExtensionManager.instance().getByPrefix(prefix);
	
				
				if (extension != null) { 
					log.getExtensions().add(extension);
				}else {
					System.err.println("Unknown extension: " + prefix);
				}
			}
		}else {
			System.err.print("extensions not deinfed correctly in file : skiping");
		}
		
		
		
			
				
	

		
		// add classfiers 
		

			JsonNode classifiers = obj.get("classifiers");
			if(classifiers != null &&  classifiers.size() != 0) {
				Iterator<String> classfierKeys = classifiers.fieldNames();
				while(classfierKeys.hasNext()) {
					String key = classfierKeys.next();
					JsonNode classfierArrayJSON = classifiers.get(key);
					String[] classfierArray = new String[classfierArrayJSON.size()];
					for(int j = 0; j < classfierArrayJSON.size();j++){
						classfierArray[j] =  classfierArrayJSON.get(j).asText();
					}
					XEventClassifier classifier = new XEventAttributeClassifier(
							key, classfierArray);
					log.getClassifiers().add(classifier);
				}
			}else {
				System.err.println("classfiers not deinfed correctly in file : skiping");
			}
		
		
		
		// add global attributes
		
		JsonNode global = obj.get("global");
		
		// add global trace
		JsonNode globalTrace = global.get("trace");
		
		Iterator<String> traceKeys = globalTrace.fieldNames();
		while(traceKeys.hasNext()) {
			String key = traceKeys.next();
			log.getGlobalTraceAttributes().add(createAttr(key,globalTrace.get(key)));
		}
		
		JsonNode globalEvent = global.get("event");
		Iterator<String> eventKeys = globalEvent.fieldNames();
		while(eventKeys.hasNext()) {
			String key = eventKeys.next();
			log.getGlobalEventAttributes().add(createAttr(key,globalEvent.get(key)));
		}	
		
		
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
		return log;
		
		
		
		
	}
	
	
//	void addAttrs(XLogBuilder builder ,JsonNode attrs) throws JSONException{
//		Iterator<String> keys = attrs.keys();
//		 
//		while(keys.hasNext()) {
//			String key = keys.next();
//			if ( attrs.get(key) instanceof Boolean) {
//				builder.addAttribute(key,(Boolean) attrs.get(key));
//			}else if (attrs.get(key) instanceof Integer) {
//				builder.addAttribute(key,(Integer) attrs.get(key));
//			}else if (attrs.get(key) instanceof Double) {
//				builder.addAttribute(key,(Double) attrs.get(key));
//			}else if (attrs.get(key) instanceof Integer) {
//				builder.addAttribute(key,(Integer) attrs.get(key));
//			}else if (attrs.get(key) instanceof JSONArray) {
//				JSONArray arr = attrs.getJSONArray(key);
//				XAttribute list = new XAttributeListImpl(key);
//				for(int i = 0; i < arr.length();i++){
//					JsonNode arrElement = (JSONObject) arr.get(i);
//					XAttribute arrAttribute = 
//					addAttrs(builder,arrElement);
//				}
//			}else if (attrs.get(key) instanceof JSONObject) {
//				JsonNode object = attrs.getJSONObject(key);
//				Iterator<String> objectKeys = object.keys();
//				
//				addAttrs(builder,object);
//			}else if (attrs.get(key) instanceof String) {
//				builder.addAttribute(key, (String) attrs.get(key) );
//			}
//			
//			
//		}
//	}
	
	
	
	XAttribute createAttr(String key, JsonNode attr) {
		XAttribute attribute = null;
		if (attr.isBoolean()) {
			attribute =  new XAttributeBooleanImpl(key, attr.asBoolean());
		}else if (attr.isInt()) {
			attribute = new XAttributeDiscreteImpl( key, attr.asInt());
		}else if (attr.isDouble()) {
			attribute = new XAttributeContinuousImpl( key,attr.asDouble());
		}else if (attr.isArray()) {
			JsonNode arr = attr;
			XAttributeListImpl list = new XAttributeListImpl(key);
			for(int i = 0; i < arr.size();i++){
				JsonNode arrElement =  arr.get(i);
				String elementKey = (String) arrElement.fieldNames().next();
				XAttribute arrAttribute = createAttr( elementKey, arrElement.get(elementKey));
				list.addToCollection(arrAttribute);
				attribute = list;
			}
		}else if ( attr.isObject()) {
			JsonNode object = attr;
			Iterator<String> objectKeys = object.fieldNames();
			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttributeContainerImpl map = new XAttributeContainerImpl(key);
			
			
			// the case of nested attributes
			if (object.size() == 2 && object.has("value") && object.has("nested-attributes") && object.get("nested-attributes") instanceof JsonNode ){
				return createNestedAttribute(key, object);
			}
			
			objectKeys = object.fieldNames();
			
						
			while(objectKeys.hasNext()) {
				String objectKey = objectKeys.next();
				XAttribute objectAttribute = createAttr(objectKey, object.get(objectKey));
				values.put(objectKey ,objectAttribute);
			}
			map.setAttributes(values);
			attribute = map;
		}else if (attr.isTextual()) {
			String text = attr.asText();
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key , date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl( key, text);
			}	
			
//			Date date = xs.parseXsDateTime(text);
//			
//			if (date != null) {
//				System.out.print(date);
//				attribute = new XAttributeTimestampImpl(key , date);
//				
//			}else {
//				Date s;
//				try {
//					s = dateFormat.parse(text);
//					attribute = new XAttributeTimestampImpl( key, s);
//				} catch (ParseException e) {
//					System.out.println("hiiiiii");
//					attribute = new XAttributeLiteralImpl(key, text);
//				}
//				
//			}	
			
//			Date date;
//			try {	
//				date = xs.parseXsDateTime(text);
//				attribute = new XAttributeTimestampImpl(key , date);
//			} catch (IllegalArgumentException e){
//				attribute = new XAttributeLiteralImpl(key, text);
//			}		
			
		}
		return attribute;
	}
	
	
	XAttribute createNestedAttribute(String key,JsonNode attr) {
		XAttribute attribute = createAttr(key, attr.get("value"));
		
		JsonNode nestedAttributes =  attr.get("nested-attributes");
		XAttributeMapImpl values = new XAttributeMapImpl();
		Iterator<String> objectKeys = nestedAttributes.fieldNames();
		while(objectKeys.hasNext()) {
			String objectKey = objectKeys.next();
			XAttribute objectAttribute = createAttr(objectKey, nestedAttributes.get(objectKey));
			values.put(objectKey,objectAttribute);
		}
		attribute.setAttributes(values);

		return attribute;
	}
	
	
}





