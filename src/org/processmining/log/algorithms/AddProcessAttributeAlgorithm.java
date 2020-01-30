package org.processmining.log.algorithms;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.log.parameters.AddProcessAttributeParameters;

public class AddProcessAttributeAlgorithm {

	/**
	 * Creates a log where every event has a new history/future attribute.
	 * 
	 * @param log
	 *            The original log.
	 * @param parameters
	 *            The configured parameters for the new attribute.
	 * @return A copy of the original log with the new attribute.
	 */
	public XLog apply(XLog log, AddProcessAttributeParameters parameters) {

		/*
		 * First, we clone the entire log.
		 */
		XLog clonedLog = (XLog) log.clone();

		/*
		 * Second, we add the attribute.
		 */
		for (XTrace trace : clonedLog) {
			/*
			 * If isBackward(), then history, else future.
			 */
			// last indicates when we're done.
			int last = parameters.isBackward() ? -1 : trace.size();
			// inc indicates the direction we walk. 
			int inc = parameters.isBackward() ? -1 : 1;

			/*
			 * For every trace.
			 */
			for (int i = 0; i < trace.size(); i++) {
				// Clear the current collection. This preserves the collection type.
				parameters.getProcessAttributeValues().clear();
				/*
				 * For every event in the correct direction.
				 */
				for (int j = i + inc; j != last; j += inc) {
					/*
					 * Skip if we already have reached the maximal collection size.
					 */
					if (parameters.getProcessAttributeValues().size() < parameters.getMaxCollectionSize()) {
						// Buffer to store value.
						StringBuffer buffer = new StringBuffer();
						// Separator between values, initially empty.
						String separator = "";
						// Gather all selected attribute values in the buffer.
						for (String attributeKey : parameters.getAttributeKeys()) {
							XAttribute attribute = trace.get(j).getAttributes().get(attributeKey);
							buffer.append(separator);
							// Update the separator.
							separator = "+";
							buffer.append(attribute != null ? attribute.toString() : "");
						}
						// The buffer now contains the value we need.Check the filter.
						String value = buffer.toString();
						if (parameters.getProcessAttributeValueFilter().contains(value)) {
							// The value passes the filter. Add it to the collection.
							parameters.getProcessAttributeValues().add(value);
						}
					}
				}
				/*
				 * Add the String value of the collection as a new literal attribute with the
				 * given key to the event.
				 */
				trace.get(i).getAttributes().put(parameters.getProcessAttributeKey(), new XAttributeLiteralImpl(
						parameters.getProcessAttributeKey(), parameters.getProcessAttributeValues().toString()));
			}
		}
		/*
		 * We're done.
		 */
		return clonedLog;
	}
}
