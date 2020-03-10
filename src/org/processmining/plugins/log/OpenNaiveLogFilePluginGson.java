package org.processmining.plugins.log;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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
		
		// set the name displayed in prom to the name of the file
		context.getFutureResult(0).setLabel(filename);
		
		//read json file
		Reader read = new InputStreamReader(input, "UTF-8");
		JsonObject obj = new JsonParser().parse(read).getAsJsonObject();
		

		
		// create a XLogBuilder to iteratively build the XLog object
		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");
		
		
//		PrintStream err = new PrintStream(new java.io.OutputStream(){
//			public void write(int b) throws IOException {
//				
//			}});
//		System.setErr(err);
//		System.setOut(err);
		
		//build all traces
		JsonArray traces = obj.getAsJsonArray("traces");
		//loop on traces
		for(int i = 0; i < traces.size();i++){
			
			JsonObject trace =  traces.get(i).getAsJsonObject();
			JsonObject traceAttrs = trace.getAsJsonObject("attrs");
			JsonArray events = trace.getAsJsonArray("events");
			
			
			builder.addTrace("t" + i);
			// loop on all trace attributes
			for(Map.Entry<String, JsonElement> entry : traceAttrs.entrySet()) {
				builder.addAttribute(createAttr(entry.getKey(),entry.getValue()));
			}

			// loop on all events of the trace
			for(int j = 0; j < events.size();j++){
				//create the event
				builder.addEvent("e" + j);
				JsonObject event =  events.get(j).getAsJsonObject();
				// create and add the event attributes
				for(Map.Entry<String, JsonElement> entry : event.entrySet()) {
					String key = entry.getKey();
					builder.addAttribute(createAttr(key,entry.getValue()));
				}
				
			}
		}
		
		// return the XLog object from the builder
		XLog log =  builder.build();
		
		//add log-attributes to the XLog object
		
		JsonObject logChildren = obj.get("log-children").getAsJsonObject();
		//create a map which will hold all log attributes 
		XAttributeMapImpl logAttributes = new XAttributeMapImpl();
		// loop on all attributes and add them to map
		for(Map.Entry<String, JsonElement> entry : logChildren.entrySet()) {
			String key = entry.getKey();
			// create attribute then add it to map
			logAttributes.put(key, createAttr(key,entry.getValue()));
		}
		// set log attributes to the map.
		log.setAttributes(logAttributes);
		
		
		// add extensions to the XLog object
		JsonArray jsonExtensions = obj.getAsJsonArray("extensions");
		if(jsonExtensions != null ) {
			//iterate over all extension 
			for(int j = 0; j < jsonExtensions.size();j++){
				XExtension extension = null;
				
				JsonObject jsonExtension = jsonExtensions.get(j).getAsJsonObject();
				//use the prefix of the extension and the extension-manger to check if the extension is known
				String prefix = jsonExtension.get("prefix").getAsString();
				extension = XExtensionManager.instance().getByPrefix(prefix);
	
				
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
		}else {
			// if the extensions array does not exist or is not defined correctly throw warning and skip parsing the extension
			System.err.print("extensions not deinfed correctly in file : skiping");
		}
		
		
		
		// add classifiers to the XLog object
		
		JsonElement classifiers = obj.get("classifiers");
		
		if(classifiers != null) {
			//get all classifier names and values
			JsonObject classfiersObject = classifiers.getAsJsonObject();
			// as <classifier-name,String array of classifier-keys>
			Set<Map.Entry<String, JsonElement>> entrySet = classfiersObject.entrySet();
			if(entrySet.size() != 0) {
				for(Map.Entry<String, JsonElement> entry : entrySet) {
					String key = entry.getKey();
					// prepare the classifier array
					JsonArray classfierArrayJSON = classfiersObject.getAsJsonArray(key);
					// turn the jsonArray to String [] 
					String[] classfierArray = new String[classfierArrayJSON.size()];
					for(int j = 0; j < classfierArrayJSON.size();j++){
						classfierArray[j] =  classfierArrayJSON.get(j).getAsString();
					}
					// use the String [] to create new XEventAttributeClassifier
					XEventClassifier classifier = new XEventAttributeClassifier(
							key, classfierArray);
					// add the new classifiers to the list of classifiers of the XLog
					log.getClassifiers().add(classifier);
				}
			}else {
				// if the classifiers object does not exist 
				System.err.println("classfiers not deinfed correctly in file : skiping");
			}
		}else {
			// or is not defined correctly throw warning and skip parsing the classifiers
			System.err.println("classfiers not deinfed correctly in file : skiping");
		}

		

			
		

		
		
		// add global attributes
		
		JsonObject global = obj.get("global").getAsJsonObject();
		
		// add global trace attributes
		JsonObject globalTrace = global.get("trace").getAsJsonObject();
		
		// loop on the attributes and add them to the globalTrace scope
		for(Map.Entry<String, JsonElement> entry : globalTrace.entrySet()) {
			String key = entry.getKey();
			log.getGlobalTraceAttributes().add(createAttr(key,entry.getValue()));
		}	
		
		
		JsonObject globalEvent = global.get("event").getAsJsonObject();
		// loop on the attributes and add them to the globalEvent scope
		for(Map.Entry<String, JsonElement> entry : globalEvent.entrySet()) {
			String key = entry.getKey();
			log.getGlobalEventAttributes().add(createAttr(key,entry.getValue()));
		}	
		
		
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
			XAttribute map = new XAttributeContainerImpl(key);
			

			// the case of nested attributes
			Set<Map.Entry<String, JsonElement>> entrySet = object.entrySet();
			
			if (entrySet.size() == 2 && object.has("value") &&  object.has("nested-attributes")){
				map = createAttr(key,object.get("value"));
				object = object.get("nested-attributes").getAsJsonObject();
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
	
}





