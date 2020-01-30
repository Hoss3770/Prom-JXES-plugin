package org.processmining.log.parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

public class AddProcessAttributeParameters {

	/*
	 * The key of the attribute to add.
	 */
	private String processAttributeKey;
	/*
	 * The key of the existing attributes to include.
	 */
	private String[] attributeKeys;
	/*
	 * The collection to use for collecting the values.
	 */
	private Collection<String> processAttributeValues;
	/*
	 * The collection of values that may be collected. 
	 */
	private Collection<String> processAttributeValueFilter;
	/*
	 * The maximal size of the collection.
	 */
	private int maxCollectionSize;
	/*
	 * Whether history (true) or future (false) attribute.
	 */
	private boolean isBackward;
	
	public AddProcessAttributeParameters(XLog log) {
		/*
		 * Set reasonable default values.
		 */
		setProcessAttributeKey("history");
		setBackward(true);
		setAttributeKeys(new String[]{ "concept:name" });
		setProcessAttributeValues(new ArrayList<String>());
		setMaxCollectionSize(1);
		Collection<String> filter = new HashSet<String>();
		for (XTrace trace : log) {
			for (XEvent event : trace) {
				XAttribute attribute = event.getAttributes().get("concept:name");
				if (attribute != null) {
					filter.add(attribute.toString());
				}
			}
		}
		setProcessAttributeValueFilter(filter);
	}
	
	/*
	 * Getters and setters.
	 */
	
	public String getProcessAttributeKey() {
		return processAttributeKey;
	}
	
	public void setProcessAttributeKey(String processAttributeKey) {
		this.processAttributeKey = processAttributeKey;
	}
	
	public String[] getAttributeKeys() {
		return attributeKeys;
	}
	
	public void setAttributeKeys(String[] attributeKeys) {
		this.attributeKeys = attributeKeys;
	}

	public Collection<String> getProcessAttributeValues() {
		return processAttributeValues;
	}

	public void setProcessAttributeValues(Collection<String> processAttributeValues) {
		this.processAttributeValues = processAttributeValues;
	}

	public int getMaxCollectionSize() {
		return maxCollectionSize;
	}

	public void setMaxCollectionSize(int maxCollectionSize) {
		this.maxCollectionSize = maxCollectionSize;
	}

	public boolean isBackward() {
		return isBackward;
	}

	public void setBackward(boolean isBackward) {
		this.isBackward = isBackward;
	}

	public Collection<String> getProcessAttributeValueFilter() {
		return processAttributeValueFilter;
	}

	public void setProcessAttributeValueFilter(Collection<String> processAttributeValueFilter) {
		this.processAttributeValueFilter = processAttributeValueFilter;
	}
	
	
}
