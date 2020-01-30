package org.processmining.plugins.log;

import java.io.InputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
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
@UIImportPlugin(description = "ProM log files JXES", extensions = {"json"})
public class OpenNaiveLogFilePluginJsonSimple extends OpenLogFilePlugin {
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
	throws Exception {
		StringWriter writer = new StringWriter();
		IOUtils.copy(input, writer);
		String theString = writer.toString();
		
		
		
		
//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//				
//			}});
//		System.setErr(err);
////		System.setOut(err);

		JSONObject obj = new JSONObject(theString);

		
//		XFactory a = new XFactoryNaiveImpl();
		
		
//		a.createAttributeBoolean("a", , arg2)
		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");
		
		
		
		
		
		JSONArray traces = obj.getJSONArray("traces");
		for(int i = 0; i < traces.length();i++){
			
			JSONObject trace = (JSONObject) traces.get(i);
			JSONObject traceAttrs = trace.getJSONObject("attrs");
			JSONArray events = trace.getJSONArray("events");
			
			builder.addTrace("t" + i);
			Iterator<String> traceAttrKeys = traceAttrs.keys();
			while(traceAttrKeys.hasNext()) {
				String key = traceAttrKeys.next();
				builder.addAttribute(createAttr(key,traceAttrs.get(key)));
			}
			
			for(int j = 0; j < events.length();j++){
				builder.addEvent("e" + j);
				JSONObject event =  (JSONObject) events.get(j);
				Iterator<String> eventAttrKeys = event.keys();
				while(eventAttrKeys.hasNext()) {
					String key = eventAttrKeys.next();
					builder.addAttribute(createAttr(key,event.get(key)));
				}
				
			}
		}
		
		XLog log =  builder.build();
		
		try {
			//add log attributes
			JSONObject logChildren = obj.getJSONObject("log-children");
			Iterator<String> logKeys = logChildren.keys();
		 
			XAttributeMapImpl logAttributes = new XAttributeMapImpl();
			while(logKeys.hasNext()) {
				String key = logKeys.next();
				logAttributes.put(key, createAttr(key,logChildren.get(key)));
			}
			
			log.setAttributes(logAttributes);
		} catch (JSONException e) {
			System.err.println("log attributes not deinfed correctly in file : skiping");
		}

		
		// add extensions
		
		try {
			JSONArray jsonExtensions = obj.getJSONArray("extensions");
			for(int j = 0; j < jsonExtensions.length();j++){
				XExtension extension = null;
				
				JSONObject jsonExtension = (JSONObject) jsonExtensions.get(j);
				
				String prefix = jsonExtension.getString("prefix");
				extension = XExtensionManager.instance().getByPrefix(jsonExtension.getString("prefix"));
	
				
				if (extension != null) { 
					log.getExtensions().add(extension);
				}else {
					System.err.println("Unknown extension: " + prefix);
				}
			}
		} catch (JSONException e) {
			System.err.println("extensions not deinfed correctly in file : skiping");
		}
		
		
		// add classfiers 
		
		try {
			JSONObject classifiers = obj.getJSONObject("classifiers");
			Iterator<String> classfierKeys = classifiers.keys();
			while(classfierKeys.hasNext()) {
				String key = classfierKeys.next();
				JSONArray classfierArrayJSON = classifiers.getJSONArray(key);
				String[] classfierArray = new String[classfierArrayJSON.length()];
				for(int j = 0; j < classfierArrayJSON.length();j++){
					classfierArray[j] = (String) classfierArrayJSON.get(j);
				}
				XEventClassifier classifier = new XEventAttributeClassifier(
						key, classfierArray);
				log.getClassifiers().add(classifier);
			}
		
		} catch (JSONException e) {
			System.err.println("classfiers not deinfed correctly in file : skiping");
		}
		
		
		// add global attributes
		
		JSONObject global = obj.getJSONObject("global");
		
		// add global trace
		JSONObject globalTrace = global.getJSONObject("trace");
		
		Iterator<String> traceKeys = globalTrace.keys();
		while(traceKeys.hasNext()) {
			String key = traceKeys.next();
			log.getGlobalTraceAttributes().add(createAttr(key,globalTrace.get(key)));
		}
		
		JSONObject globalEvent = global.getJSONObject("event");
		Iterator<String> eventKeys = globalEvent.keys();
		while(eventKeys.hasNext()) {
			String key = eventKeys.next();
			log.getGlobalEventAttributes().add(createAttr(key,globalEvent.get(key)));
		}	
		
		
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
	
	
	
	XAttribute createAttr(String key, Object attr) throws JSONException {
		XAttribute attribute = null;
		if (attr instanceof Boolean) {
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
			
			Iterator<String> objectKeys = object.keys();
			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttributeContainerImpl map = new XAttributeContainerImpl(key);
			
			
			// the case of nested attributes
			if (object.length() == 2 && object.has("value") && object.has("nested-attributes") && object.get("nested-attributes") instanceof JSONObject ){
				return createNestedAttribute(key, object);
			}
			
			objectKeys = object.keys();
			
			
			
			
			
			while(objectKeys.hasNext()) {
				String objectKey = objectKeys.next();
				XAttribute objectAttribute = createAttr(objectKey, object.get(objectKey));
				values.put(objectKey ,objectAttribute);
			}
			map.setAttributes(values);
			attribute = map;
		}else if (attr instanceof String) {
			String text = (String) attr;
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key , date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl( key, text);
			}	
			
		}
		return attribute;
	}
	
	
	XAttribute createNestedAttribute(String key,JSONObject attr) throws JSONException {
		XAttribute attribute = createAttr(key, attr.get("value"));
		
		JSONObject nestedAttributes =  attr.getJSONObject("nested-attributes");
		XAttributeMapImpl values = new XAttributeMapImpl();
		Iterator<String> objectKeys = nestedAttributes.keys();
		while(objectKeys.hasNext()) {
			String objectKey = objectKeys.next();
			XAttribute objectAttribute = createAttr(objectKey, nestedAttributes.get(objectKey));
			values.put(objectKey,objectAttribute);
		}

		attribute.setAttributes(values);
		return attribute;
	}
	
	
}





