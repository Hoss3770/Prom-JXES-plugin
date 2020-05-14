package org.processmining.plugins.log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
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

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.DecodingMode;

@Plugin(name = "Open JXES (Jsoninter)", level = PluginLevel.PeerReviewed, parameterLabels = { "Filename" }, returnLabels = {
		"Log (single process)" }, returnTypes = { XLog.class })
@UIImportPlugin(description = "ProM log files JXES (Jsoninter)", extensions = { "json" })
public class OpenNaiveLogFilePluginJsoninter_old_3 extends OpenLogFilePlugin {
	private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS");

	protected Object importFromStream(PluginContext context, InputStream input, String filename, long fileSizeInBytes)
			throws Exception {
		
		JsonIterator.setMode(DecodingMode.DYNAMIC_MODE_AND_MATCH_FIELD_WITH_HASH);
		
		
		BufferedReader bR = new BufferedReader(  new InputStreamReader(input));
		String line = "";

		StringBuilder responseStrBuilder = new StringBuilder();
		while((line =  bR.readLine()) != null){

		    responseStrBuilder.append(line);
		}
		input.close();
		Any obj = JsonIterator.deserialize( responseStrBuilder.toString());

		

		//		PrintStream err = new PrintStream(new java.io.OutputStream(){
		//			public void write(int b) throws IOException {
		//				
		//			}});
		//		System.setErr(err);
		////		System.setOut(err);

		
		


		XLogBuilder builder = XLogBuilder.newInstance().startLog("JXES-log");

		Any traces = obj.get("traces");
		int i = -1;
		for (Any trace : traces) {
			i += 1;
			Any traceAttrs = trace.get("attrs");
			Any events = trace.get("events");
			builder.addTrace("t" + i);
			Set<String> traceAttrKeys = traceAttrs.keys();
			for (String key : traceAttrKeys) {
				builder.addAttribute(createAttr(key, traceAttrs.get(key)));
			}

			int j = -1;
			for (Any event : events) {
				Set<String> eventAttrKeys = event.keys();
				for (String key : eventAttrKeys) {
					builder.addAttribute(createAttr(key, event.get(key)));
				}

			}
		}

		
		XLog log = builder.build();
		
		//add log attributes
		Any logChildren = obj.get("log-children");
		Set<String> logKeys = logChildren.keys();
		XAttributeMapImpl logAttributes = new XAttributeMapImpl();
		
		for (String key : logKeys) {
			logAttributes.put(key,createAttr(key, logChildren.get(key)));
		}
		log.setAttributes(logAttributes);
		
		
		

		//		System.err.println("log attributes not deinfed correctly in file : skiping");

		// add extensions

		Any jsonExtensions = obj.get("extensions");
		if(jsonExtensions.valueType() != ValueType.NULL) {
			for (Any jsonExtension : jsonExtensions) {
	
				XExtension extension = null;
	
				String prefix = jsonExtension.get("prefix").toString();
				extension = XExtensionManager.instance().getByPrefix(prefix);
	
				if (extension != null) {
					log.getExtensions().add(extension);
				} else {
					System.err.println("Unknown extension: " + prefix);
				}
			}
		}else {
			System.err.println("extensions not deinfed correctly in file : skiping");
		}

		// add classfiers 

		Any classifiers = obj.get("classifiers");
		Set<String> classfierKeys = classifiers.keys();
		if(classifiers.valueType() != ValueType.NULL && classfierKeys.size() != 0 ) {
			for (String key : classfierKeys) {
				String[] classfierArray = classifiers.get(key).as(String[].class);
				XEventClassifier classifier = new XEventAttributeClassifier(key, classfierArray);
				log.getClassifiers().add(classifier);
			}
		}else{
			System.err.println("classfiers not deinfed correctly in file : skiping");
		}

		// add global attributes

		Any global = obj.get("global");

		// add global trace
		Any globalTrace = global.get("trace");

		Set<String> traceKeys = globalTrace.keys();
		for (String key : traceKeys) {
			log.getGlobalTraceAttributes().add(createAttr(key, globalTrace.get(key)));
		}

		Any globalEvent = global.get("event");
		Set<String> eventKeys = globalEvent.keys();
		for (String key : eventKeys) {
			log.getGlobalEventAttributes().add(createAttr(key, globalEvent.get(key)));
		}

		return log;
		
	}

	XAttribute createAttr(String key, Any attr) {
		
		XAttribute attribute = null;
		if (attr.valueType() == ValueType.BOOLEAN) {
			System.out.println(attr.getClass());
			attribute = new XAttributeBooleanImpl(key, attr.toBoolean());
		} else if (attr.valueType() == ValueType.NUMBER) {
			
			double number = attr.toDouble();
			if (number % 1 == 0) {
				System.out.println(attr.getClass());
				attribute = new XAttributeDiscreteImpl(key, (long) number);
			} else {
				System.out.println(attr.getClass());
				attribute = new XAttributeContinuousImpl(key, number);
			}
		} else if (attr.valueType() == ValueType.ARRAY) {
			System.out.println(attr.object().getClass());
			System.out.println(attr.getClass());
			XAttributeListImpl list = new XAttributeListImpl(key);
			for(Any arrElement :  attr){
				String elementKey = (String) arrElement.keys().iterator().next();
				XAttribute arrAttribute = createAttr(elementKey, arrElement.get(elementKey));
				list.addToCollection(arrAttribute);	
			}
			attribute = list;
		} else if (attr.valueType() == ValueType.OBJECT) {
			
			System.out.println(attr.object().getClass());
			Set<String> objectKeys = attr.keys();
			XAttributeMapImpl values = new XAttributeMapImpl();
			XAttributeContainerImpl map = new XAttributeContainerImpl(key);
			
			// the case of nested attributes
			if (attr.size() == 2 && attr.get("value").valueType() != ValueType.INVALID && attr.get("nested-attributes").valueType() != ValueType.INVALID
					&& attr.get("nested-attributes").valueType() == ValueType.OBJECT) {
				return createNestedAttribute(key, attr);
			}

			objectKeys = attr.keys();

			for(String objecgtKey : objectKeys){
				XAttribute objectAttribute = createAttr(objecgtKey, attr.get(objecgtKey));
				values.put(objecgtKey, objectAttribute);
			}
			map.setAttributes(values);
			attribute = map;
		} else if (attr.valueType() == ValueType.STRING) {
			
			String text = attr.toString();
			Date date;
			try {
				date = DateUtil.parse(text);
				attribute = new XAttributeTimestampImpl(key, date);
			} catch (ParseException e) {
				attribute = new XAttributeLiteralImpl(key, text);
			}

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

	XAttribute createNestedAttribute(String key, Any attr){
		XAttribute attribute = createAttr(key, attr.get("value"));

		Any nestedAttributes = attr.get("nested-attributes");
		XAttributeMapImpl values = new XAttributeMapImpl();
		
		Set<String> objectKeys = nestedAttributes.keys();
		for(String objectKey : objectKeys){
			XAttribute objectAttribute = createAttr(objectKey, nestedAttributes.get(objectKey));
			values.put(objectKey, objectAttribute);
		}

		attribute.setAttributes(values);
		return attribute;
	}

}
