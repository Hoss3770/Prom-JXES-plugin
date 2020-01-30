package org.processmining.plugins.log;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Set;

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
import org.processmining.contexts.uitopia.annotations.UIImportPlugin;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginLevel;
import org.processmining.log.utils.XLogBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser; 

@Plugin(name = "Open JXES", level = PluginLevel.PeerReviewed, parameterLabels = { "Filename" }, returnLabels = { "Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (GSON)", extensions = {"json"})
public class OpenNaiveLogFilePluginGson extends OpenLogFilePlugin {
	
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");
	
	
	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
	throws Exception {
		StringWriter writer = new StringWriter();
		Gson gson = new Gson();
	
		
//		ObjectMapper mapper = new ObjectMapper();
		Reader read = new InputStreamReader(input, "UTF-8");
		JsonObject obj = new JsonParser().parse(read).getAsJsonObject();
		

		
//		XFactory a = new XFactoryNaiveImpl();
		
		
//		a.createAttributeBoolean("a", , arg2)
		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");
		
		
//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//				
//			}});
//		System.setErr(err);
//		System.setOut(err);
		
		
		JsonArray traces = obj.getAsJsonArray("traces");
		for(int i = 0; i < traces.size();i++){
			
			JsonObject trace =  traces.get(i).getAsJsonObject();
			JsonObject traceAttrs = trace.getAsJsonObject("attrs");
			JsonArray events = trace.getAsJsonArray("events");
			
			builder.addTrace("t" + i);
			for(Map.Entry<String, JsonElement> entry : traceAttrs.entrySet()) {
				builder.addAttribute(createAttr(entry.getKey(),entry.getValue()));
			}

			
			for(int j = 0; j < events.size();j++){
				builder.addEvent("e" + j);
				JsonObject event =  events.get(j).getAsJsonObject();
				for(Map.Entry<String, JsonElement> entry : event.entrySet()) {
					String key = entry.getKey();
					builder.addAttribute(createAttr(key,entry.getValue()));
				}
				
			}
		}
		
		XLog log =  builder.build();
		
	
			//add log attributes
			JsonObject logChildren = obj.get("log-children").getAsJsonObject();
			XAttributeMapImpl logAttributes = new XAttributeMapImpl();
			for(Map.Entry<String, JsonElement> entry : logChildren.entrySet()) {
				String key = entry.getKey();
				logAttributes.put(key, createAttr(key,entry.getValue()));
			}
			
			log.setAttributes(logAttributes);
		

		
		// add extensions
		
			
				JsonArray jsonExtensions = obj.getAsJsonArray("extensions");
				if(jsonExtensions != null ) {
					for(int j = 0; j < jsonExtensions.size();j++){
						XExtension extension = null;
						
						JsonObject jsonExtension = jsonExtensions.get(j).getAsJsonObject();
						
						String prefix = jsonExtension.get("prefix").getAsString();
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
		

			JsonElement classifiers = obj.get("classifiers");
			
			if(classifiers != null) {
				JsonObject classfiersObject = classifiers.getAsJsonObject();
				Set<Map.Entry<String, JsonElement>> entrySet = classfiersObject.entrySet();
				if(entrySet.size() != 0) {
					for(Map.Entry<String, JsonElement> entry : entrySet) {
						String key = entry.getKey();
						JsonArray classfierArrayJSON = classfiersObject.getAsJsonArray(key);
						String[] classfierArray = new String[classfierArrayJSON.size()];
						for(int j = 0; j < classfierArrayJSON.size();j++){
							classfierArray[j] =  classfierArrayJSON.get(j).getAsString();
						}
						XEventClassifier classifier = new XEventAttributeClassifier(
								key, classfierArray);
						log.getClassifiers().add(classifier);
					}
				}else {
					System.err.println("classfiers not deinfed correctly in file : skiping");
				}
			}else {
				System.err.println("classfiers not deinfed correctly in file : skiping");
			}
		

		
		
		// add global attributes
		
		JsonObject global = obj.get("global").getAsJsonObject();
		
		// add global trace
		JsonObject globalTrace = global.get("trace").getAsJsonObject();
		
		
		for(Map.Entry<String, JsonElement> entry : globalTrace.entrySet()) {
			String key = entry.getKey();
			log.getGlobalTraceAttributes().add(createAttr(key,entry.getValue()));
		}	
		
		JsonObject globalEvent = global.get("event").getAsJsonObject();
		for(Map.Entry<String, JsonElement> entry : globalEvent.entrySet()) {
			String key = entry.getKey();
			log.getGlobalEventAttributes().add(createAttr(key,entry.getValue()));
		}	
		
		
		return log;
		
		
		
		
	}
	
	
//	void addAttrs(XLogBuilder builder ,JsonObject attrs) throws JSONException{
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
//					JsonObject arrElement = (JSONObject) arr.get(i);
//					XAttribute arrAttribute = 
//					addAttrs(builder,arrElement);
//				}
//			}else if (attrs.get(key) instanceof JSONObject) {
//				JsonObject object = attrs.getJSONObject(key);
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
	
	
	
	XAttribute createAttr(String key, JsonElement attr) {
		XAttribute attribute = null;
		if(attr.isJsonPrimitive()) {
			if (attr.getAsJsonPrimitive().isBoolean()) {
				attribute =  new XAttributeBooleanImpl(key, attr.getAsBoolean());
			}else if (attr.getAsJsonPrimitive().isNumber()) {
				Double number = attr.getAsJsonPrimitive().getAsDouble();
				if(number % 1 == 0) { 
					attribute = new XAttributeDiscreteImpl(key, attr.getAsJsonPrimitive().getAsInt());
				}else {
					attribute = new XAttributeContinuousImpl(key,number);
				}
			}else if (attr.getAsJsonPrimitive().isString()) {
				String text = attr.getAsJsonPrimitive().getAsString();
				Date date;
				try {
					date = DateUtil.parse(text);
					attribute = new XAttributeTimestampImpl(key , date);
				} catch (ParseException e) {
					attribute = new XAttributeLiteralImpl( key, text);
				}	
				
			}
		}
		 if (attr.isJsonArray()) {
			JsonArray arr = attr.getAsJsonArray();
			XAttributeListImpl list = new XAttributeListImpl(key);
			for(int i = 0; i < arr.size();i++){
				JsonElement arrElement =  arr.get(i);
				for(Map.Entry<String, JsonElement> entry : arrElement.getAsJsonObject().entrySet()) {
					XAttribute arrAttribute = createAttr( entry.getKey(), entry.getValue());
					list.addToCollection(arrAttribute);
				}
				attribute = list;
			}
		}else if (attr.isJsonObject()) {
			JsonObject object = attr.getAsJsonObject();
			
			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttributeContainerImpl map = new XAttributeContainerImpl(key);
			

			// the case of nested attributes
			Set<Map.Entry<String, JsonElement>> entrySet = object.entrySet();
			
			if (entrySet.size() == 2 && object.has("value") &&  object.has("nested-attributes") && object.get("nested-attributes").isJsonObject() ){
				return createNestedAttribute(key, object);
			}
			
			
			
			
			for(Map.Entry<String, JsonElement> entry : entrySet) {
				String objectKey = entry.getKey();
				XAttribute objectAttribute = createAttr(objectKey, entry.getValue());
				values.put(objectKey ,objectAttribute);
			}
			map.setAttributes(values);
			attribute = map;
		}
		return attribute;
	}
	
	
	XAttribute createNestedAttribute(String key,JsonObject attr) {
		XAttribute attribute = createAttr(key, attr.get("value"));
		
		JsonObject nestedAttributes =  attr.get("nested-attributes").getAsJsonObject();
		XAttributeMapImpl values = new XAttributeMapImpl();
		
		for(Map.Entry<String, JsonElement> entry : nestedAttributes.entrySet()) {
		  
			String objectKey = entry.getKey();
			XAttribute objectAttribute = createAttr(objectKey,entry.getValue());
			values.put(objectKey,objectAttribute);
		}
		attribute.setAttributes(values);
		return attribute;
	}
	
	
}





