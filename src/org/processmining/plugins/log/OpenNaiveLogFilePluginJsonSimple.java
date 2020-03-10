package org.processmining.plugins.log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang3.time.FastDateFormat;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.processmining.contexts.uitopia.annotations.UIImportPlugin;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginLevel;
import org.processmining.log.utils.XLogBuilder; 

@Plugin(name = "Open JXES", level = PluginLevel.PeerReviewed, parameterLabels = { "Filename" }, returnLabels = { "Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (simple) ", extensions = {"json"})
public class OpenNaiveLogFilePluginJsonSimple extends OpenLogFilePlugin {
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
	throws Exception {
		
		// set the name displayed in prom to the name of the file
		context.getFutureResult(0).setLabel(filename);
		
//		StringWriter writer = new StringWriter();
//		IOUtils.copy(input, writer);
//		String theString = writer.toString();
//		JSONObject obj = new JSONObject(theString);
		
		
		BufferedReader bR = new BufferedReader(  new InputStreamReader(input));
		String line = "";

		StringBuilder responseStrBuilder = new StringBuilder();
		while((line =  bR.readLine()) != null){

		    responseStrBuilder.append(line);
		}
		input.close();

		JSONObject obj= new JSONObject(responseStrBuilder.toString());
		
		//parse json object through json tokener
//		JSONObject obj = new JSONObject(new JSONTokener(input));
		
		
//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//				
//			}});
//		System.setErr(err);
////		System.setOut(err);

	
		
		// create a XLogBuilder to iteratively build the XLog 
		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");
		
		
		
		//build all traces
		JSONArray traces = obj.getJSONArray("traces");
		for(int i = 0; i < traces.length();i++){
			
			//get trace as JSONObject
			JSONObject trace = (JSONObject) traces.get(i);
			// get trace-attributes
			JSONObject traceAttrs = trace.getJSONObject("attrs");
			//get all trace-events
			JSONArray traceEvents = trace.getJSONArray("events");
			// add trace to the log with an identifier 
			builder.addTrace("t" + i);
			
			// get all keys (attribute names)
			Iterator<String> traceAttrKeys = traceAttrs.keys();
			// iterate and create every trace attribute , by passing the name (key) and the value to the createAttr method 
			while(traceAttrKeys.hasNext()) {
				String key = traceAttrKeys.next();
				builder.addAttribute(createAttr(key,traceAttrs.get(key)));
			}
			
			// iterate on the events and create all trace-events.
			for(int j = 0; j < traceEvents.length();j++){
				builder.addEvent("e" + j);
				JSONObject event =  (JSONObject) traceEvents.get(j);
				Iterator<String> eventAttrKeys = event.keys();
				while(eventAttrKeys.hasNext()) {
					String key = eventAttrKeys.next();
					builder.addAttribute(createAttr(key,event.get(key)));
				}
				
			}
		}
		
		// return the XLog object from the builder
		XLog log =  builder.build();
		
		
		//add log-attributes to the XLog object
		try {
			// get the log-children object (which contains the log-attributes)
			JSONObject logChildren = obj.getJSONObject("log-children");
			
			// get the attribute names
			Iterator<String> logKeys = logChildren.keys();
			
			// create an AttributeMap which contains all of the log-attributes
			XAttributeMapImpl logAttributes = new XAttributeMapImpl();
			
			// iterate and add the attributes to the map
			while(logKeys.hasNext()) {
				String key = logKeys.next();
				logAttributes.put(key, createAttr(key,logChildren.get(key)));
			}
			
			// set the log attributes to the AttributeMap
			log.setAttributes(logAttributes);
			
		} catch (JSONException e) {
			// if the log-children object does not exist or is not defined correctly throw warning and skip parsing the log-attributes
			System.err.println("log attributes not deinfed correctly in file : skiping");
		}

		
		// add extensions to the XLog object
		try {
			//iterate over all extension 
			JSONArray jsonExtensions = obj.getJSONArray("extensions");
			for(int j = 0; j < jsonExtensions.length();j++){
				XExtension extension = null;
				
				JSONObject jsonExtension = (JSONObject) jsonExtensions.get(j);
				
				//use the prefix of the extension and the extension-manger to check if the extension is known
				String prefix = jsonExtension.getString("prefix");
				extension = XExtensionManager.instance().getByPrefix(jsonExtension.getString("prefix"));
	
				
				if (extension != null) { 
					//extension known
					// add to the list of extensions of the XLog
					log.getExtensions().add(extension);
				}else {
					//extension was not found --> unknown
					// throw warning and skip extension 
					System.err.println("Unknown extension: " + prefix);
				}
			}
		} catch (JSONException e) {
			// if the extensions array does not exist or is not defined correctly throw warning and skip parsing the extension
			System.err.println("extensions not deinfed correctly in file : skiping");
		}
		
		
		// add classifiers to the XLog object
		try {
			JSONObject classifiers = obj.getJSONObject("classifiers");
			//get all classifier names
			Iterator<String> classfierKeys = classifiers.keys();
			while(classfierKeys.hasNext()) {
				String key = classfierKeys.next();
				// prepare the classifier array
				JSONArray classfierArrayJSON = classifiers.getJSONArray(key);
				String[] classfierArray = new String[classfierArrayJSON.length()];
				for(int j = 0; j < classfierArrayJSON.length();j++){
					classfierArray[j] = (String) classfierArrayJSON.get(j);
				}
				// use the String [] to create new XEventAttributeClassifier
				XEventClassifier classifier = new XEventAttributeClassifier(
						key, classfierArray);
				// add the new classifiers to the list of classifiers of the XLog
				log.getClassifiers().add(classifier);
			}
		
		} catch (JSONException e) {
			// if the classifiers object does not exist or is not defined correctly throw warning and skip parsing the classifiers
			System.err.println("classfiers not deinfed correctly in file : skiping");
		}
		
		
		// add global attributes
		
		JSONObject global = obj.getJSONObject("global");
		
		
		// add global trace attributes
		JSONObject globalTrace = global.getJSONObject("trace");
		
		Iterator<String> traceKeys = globalTrace.keys();
		while(traceKeys.hasNext()) {
			String key = traceKeys.next();
			log.getGlobalTraceAttributes().add(createAttr(key,globalTrace.get(key)));
		}
		
		// add global event attributes
		JSONObject globalEvent = global.getJSONObject("event");
		Iterator<String> eventKeys = globalEvent.keys();
		while(eventKeys.hasNext()) {
			String key = eventKeys.next();
			log.getGlobalEventAttributes().add(createAttr(key,globalEvent.get(key)));
		}	
		
		System.out.println("Memory used: " +  ((double)( Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory()) / (double) (1024 * 1024)));
		return log;
		
		
		
	}
	
	
//	void addAttrs(XLogBuilder builder ,JSONObject attrs) throws JSONException{
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
//					JSONObject arrElement = (JSONObject) arr.get(i);
//					XAttribute arrAttribute = 
//					addAttrs(builder,arrElement);
//				}
//			}else if (attrs.get(key) instanceof JSONObject) {
//				JSONObject object = attrs.getJSONObject(key);
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
	
	
	/**
	 * 
	 * Depending on the type of the attribute given as a parameter the right attribute implementation is used.
	 * 
	 * @param the attribute name
	 * @param the attribute value	 
	 * @return an attribute ready to be added to the log object
	 * @see org.deckfour.xes.model.XAttribute
	 */
	
	XAttribute createAttr(String key, Object attr) throws JSONException {
		XAttribute attribute = null;
		if (attr instanceof String) {
			String text = (String) attr;
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key , date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl( key, text);
			}	
			
		}else if (attr instanceof Boolean) {
			attribute =  new XAttributeBooleanImpl(key, (Boolean) attr);
		}else if (attr instanceof Integer) {
			attribute = new XAttributeDiscreteImpl( key, (Integer)attr );
		}else if (attr instanceof Double) {
			attribute = new XAttributeContinuousImpl( key, (Double) attr);
		}else if (attr instanceof JSONArray) {
			JSONArray arr = (JSONArray) attr;
			XAttributeListImpl list = new XAttributeListImpl(key);
			for(int i = 0; i < arr.length();i++){
				JSONObject arrElement = (JSONObject) arr.get(i);
				String elementKey = (String) arrElement.keys().next();
				XAttribute arrAttribute = createAttr( elementKey, arrElement.get(elementKey));
				list.addToCollection(arrAttribute);
				attribute = list;
			}
		}else if (attr instanceof JSONObject) {
			JSONObject object = (JSONObject) attr;	
			
			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttribute map = new XAttributeContainerImpl(key);
			
			
			// the case of nested attributes
			if (object.length() == 2 && object.has("value") && object.has("nested-attributes")){
				
				map = createAttr(key,attr.get("value"));
				object = (JSONObject) object.get("nested-attributes");
				
			}
			
			Iterator<String> objectKeys = object.keys();
						
			
			while(objectKeys.hasNext()) {
				String objectKey = objectKeys.next();
				XAttribute objectAttribute = createAttr(objectKey, object.get(objectKey));
				values.put(objectKey ,objectAttribute);
			}
			map.setAttributes(values);
			attribute = map;
		}
		return attribute;
	}
	
	
	
//	XAttribute createNestedAttribute(String key,JSONObject attr) throws JSONException {
//		XAttribute attribute = createAttr(key, attr.get("value"));
//		
//		JSONObject nestedAttributes =  attr.getJSONObject("nested-attributes");
//		XAttributeMapImpl values = new XAttributeMapImpl();
//		Iterator<String> objectKeys = nestedAttributes.keys();
//		while(objectKeys.hasNext()) {
//			String objectKey = objectKeys.next();
//			XAttribute objectAttribute = createAttr(objectKey, nestedAttributes.get(objectKey));
//			values.put(objectKey,objectAttribute);
//		}
//
//		attribute.setAttributes(values);
//		return attribute;
//	}
	
	
}





