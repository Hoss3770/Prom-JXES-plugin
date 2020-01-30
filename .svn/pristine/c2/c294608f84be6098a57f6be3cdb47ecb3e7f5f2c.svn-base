package org.processmining.log.csvexport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension.StandardModel;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.out.XSerializer;
import org.processmining.contexts.uitopia.UIPluginContext;

public class ExportLogCsvAlgorithm {

	protected void export(UIPluginContext context, XLog log, File file, boolean sparse) throws IOException {
		FileOutputStream out = new FileOutputStream(file);

		long instanceNumber=1;

		//final XLog result = XFactoryRegistry.instance().currentDefault().createLog(log.getAttributes());
		final XLifecycleExtension lfExt = XLifecycleExtension.instance();
		final XFactory factory=XFactoryRegistry.instance().currentDefault();
		final XConceptExtension cpExt=XConceptExtension.instance();
		final HashMap<String,List<Long>> map=new HashMap<String, List<Long>>();
		String activityName;
		
		for (XTrace trace : log) {
			map.clear();
			for (XEvent event : trace) {
				StandardModel transition = lfExt.extractStandardTransition(event);
				if (transition == null) {
					// No lifecycle transition  information. Ignore.
					continue;
				}
				switch(transition)
				{
					case START :
						activityName=cpExt.extractName(event);
						if (activityName!=null)
						{
							//event=factory.createEvent(e.getAttributes());
							if (cpExt.extractInstance(event)==null)
							{						
								List<Long> listInstances=map.get(activityName);
								if (listInstances==null)
								{
									listInstances=new LinkedList<Long>();
									map.put(activityName, listInstances);
								}
								cpExt.assignInstance(event, String.valueOf(instanceNumber));
								listInstances.add(instanceNumber++);
							}
						}
						break;					
					case COMPLETE :
						activityName=cpExt.extractName(event);
						if (activityName!=null)
						{
							event=factory.createEvent(event.getAttributes());							
							if (cpExt.extractInstance(event)==null)
							{
								List<Long> listInstances=map.get(activityName);
								if (listInstances==null || listInstances.isEmpty())									
									cpExt.assignInstance(event, String.valueOf(instanceNumber++));
								else
								{
									cpExt.assignInstance(event, String.valueOf(listInstances.remove(0)));
								}
							}
						}
						break;
					default :
						//event=null;
						break;
				}
				//copy.add(event);
			}
		}
		XSerializer logSerializer = new XesCsvSerializer("yyyy/MM/dd HH:mm:ss.SSS", sparse);
		logSerializer.serialize(log, out);
		out.close();	
	}
}
