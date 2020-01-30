package org.processmining.log.dialogs;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.ListSelectionModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.processmining.framework.util.collection.TreeMultiSet;
import org.processmining.framework.util.ui.widgets.ProMComboBox;
import org.processmining.framework.util.ui.widgets.ProMList;
import org.processmining.framework.util.ui.widgets.ProMTextField;
import org.processmining.log.parameters.AddProcessAttributeParameters;

import com.fluxicon.slickerbox.components.NiceSlider;
import com.fluxicon.slickerbox.components.NiceSlider.Orientation;
import com.fluxicon.slickerbox.factory.SlickerFactory;

import info.clearthought.layout.TableLayout;

public class AddProcessAttributeDialog extends JPanel {

	/**
	 * Dialog that allows the user to configure the parameters for adding a
	 * history/future parameter.
	 */
	private static final long serialVersionUID = -7410794120172073533L;
	/*
	 * Keep track of the old values filter.
	 */
	private Component oldValuesList = null;

	public AddProcessAttributeDialog(final XLog log, final AddProcessAttributeParameters parameters) {
		double size[][] = { { 300, TableLayout.FILL },
				{ 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, TableLayout.FILL } };
		setLayout(new TableLayout(size));
		int y = 0;

		/*
		 * Add a combo box for history/future.
		 */
		this.add(new JLabel("Select whether history or future:"), "0, " + (y++));
		ComboBoxModel<String> comboBoxModelHistory = new DefaultComboBoxModel<String>(
				new String[] { "History", "Future" });
		final ProMComboBox<String> comboBoxHistory = new ProMComboBox<String>(comboBoxModelHistory);
		comboBoxHistory.setSelectedIndex(parameters.isBackward() ? 0 : 1);
		comboBoxHistory.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				parameters.setBackward(comboBoxHistory.getSelectedItem().equals("History"));
			}

		});
		comboBoxHistory.setPreferredSize(new Dimension(500, 30));
		this.add(comboBoxHistory, "0, " + (y++));

		/*
		 * Add a text field for the new key name.
		 */
		this.add(new JLabel("Enter a name for the new attribute:"), "0, " + (y++));
		final ProMTextField processAttributeKeyTextField = new ProMTextField(parameters.getProcessAttributeKey());
		this.add(processAttributeKeyTextField, "0, " + (y++));
		processAttributeKeyTextField.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				parameters.setProcessAttributeKey(processAttributeKeyTextField.getText());
			}
		});
		processAttributeKeyTextField.setPreferredSize(new Dimension(500, 30));

		/*
		 * Add a combo box for the attribute keys to be included. The first key is
		 * required, the others are optional.
		 */
		this.add(new JLabel("Select a first attribute key (required):"), "0, " + (y++));
		Set<String> attributeKeys = new TreeSet<String>();
		if (log.size() > 0) {
			XTrace trace = log.iterator().next();
			for (XEvent event : trace) {
				attributeKeys.addAll(event.getAttributes().keySet());
			}
		} else { // Add some default keys...
			attributeKeys.add("concept:name");
			attributeKeys.add("lifecycle:transition");
			attributeKeys.add("org:resource");
		}
		String[] attributeKeyList = new String[attributeKeys.size()];
		String[] attributeKeyList2 = new String[attributeKeys.size() + 1];
		int i = 0;
		attributeKeyList2[i++] = "<None>";
		for (String key : attributeKeys) {
			attributeKeyList[i - 1] = key;
			attributeKeyList2[i++] = key;
		}
		ComboBoxModel<String> comboBoxModelAttributeKey = new DefaultComboBoxModel<String>(attributeKeyList);
		ComboBoxModel<String> comboBoxModelAttributeKey2 = new DefaultComboBoxModel<String>(attributeKeyList2);
		ComboBoxModel<String> comboBoxModelAttributeKey3 = new DefaultComboBoxModel<String>(attributeKeyList2);
		final ProMComboBox<String> comboBoxAttributeKey = new ProMComboBox<String>(comboBoxModelAttributeKey);
		final ProMComboBox<String> comboBoxAttributeKey2 = new ProMComboBox<String>(comboBoxModelAttributeKey2);
		final ProMComboBox<String> comboBoxAttributeKey3 = new ProMComboBox<String>(comboBoxModelAttributeKey3);
		comboBoxAttributeKey.setSelectedIndex(0);
		comboBoxAttributeKey2.setSelectedIndex(0);
		comboBoxAttributeKey3.setSelectedIndex(0);
		String[] keyList = new String[1];
		keyList[0] = attributeKeyList[0];
		parameters.setAttributeKeys(keyList);
		updateValues(log, parameters);
		comboBoxAttributeKey.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				String key = (String) comboBoxAttributeKey.getSelectedItem();
				String key2 = (String) comboBoxAttributeKey2.getSelectedItem();
				String key3 = (String) comboBoxAttributeKey3.getSelectedItem();
				int n = 1;
				if (!key2.equals("<None>")) {
					n++;
				}
				if (!key3.equals("<None>")) {
					n++;
				}
				String[] keyList = new String[n];
				n = 0;
				keyList[n++] = key;
				if (!key2.equals("<None>")) {
					keyList[n++] = key2;
				}
				if (!key3.equals("<None>")) {
					keyList[n++] = key3;
				}
				parameters.setAttributeKeys(keyList);
				updateValues(log, parameters);
			}

		});
		comboBoxAttributeKey2.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				String key = (String) comboBoxAttributeKey.getSelectedItem();
				String key2 = (String) comboBoxAttributeKey2.getSelectedItem();
				String key3 = (String) comboBoxAttributeKey3.getSelectedItem();
				int n = 1;
				if (!key2.equals("<None>")) {
					n++;
				}
				if (!key3.equals("<None>")) {
					n++;
				}
				String[] keyList = new String[n];
				n = 0;
				keyList[n++] = key;
				if (!key2.equals("<None>")) {
					keyList[n++] = key2;
				}
				if (!key3.equals("<None>")) {
					keyList[n++] = key3;
				}
				parameters.setAttributeKeys(keyList);
				updateValues(log, parameters);
			}

		});
		comboBoxAttributeKey3.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				String key = (String) comboBoxAttributeKey.getSelectedItem();
				String key2 = (String) comboBoxAttributeKey2.getSelectedItem();
				String key3 = (String) comboBoxAttributeKey3.getSelectedItem();
				int n = 1;
				if (!key2.equals("<None>")) {
					n++;
				}
				if (!key3.equals("<None>")) {
					n++;
				}
				String[] keyList = new String[n];
				n = 0;
				keyList[n++] = key;
				if (!key2.equals("<None>")) {
					keyList[n++] = key2;
				}
				if (!key3.equals("<None>")) {
					keyList[n++] = key3;
				}
				parameters.setAttributeKeys(keyList);
				updateValues(log, parameters);
			}

		});
		comboBoxAttributeKey.setPreferredSize(new Dimension(500, 30));
		comboBoxAttributeKey2.setPreferredSize(new Dimension(500, 30));
		comboBoxAttributeKey3.setPreferredSize(new Dimension(500, 30));
		this.add(comboBoxAttributeKey, "0, " + (y++));
		this.add(new JLabel("Select additional attribute keys (optional):"), "0, " + (y++));
		this.add(comboBoxAttributeKey2, "0, " + (y++));
		this.add(comboBoxAttributeKey3, "0, " + (y++));

		/*
		 * Add a combo box to select a collection type.
		 */
		this.add(new JLabel("Select collection type:"), "0, " + (y++));
		ComboBoxModel<String> comboBoxModelCollection = new DefaultComboBoxModel<String>(
				new String[] { "List", "Set", "Bag" });
		final ProMComboBox<String> comboBoxCollection = new ProMComboBox<String>(comboBoxModelCollection);
		if (parameters.getProcessAttributeValues() instanceof TreeMultiSet) {
			comboBoxCollection.setSelectedIndex(1);
		} else if (parameters.getProcessAttributeValues() instanceof TreeSet) {
			comboBoxCollection.setSelectedIndex(2);
		} else {
			comboBoxCollection.setSelectedIndex(0);
		}
		comboBoxCollection.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				if (comboBoxCollection.getSelectedItem().equals("Set")) {
					parameters.setProcessAttributeValues(new TreeSet<String>());
				} else if (comboBoxCollection.getSelectedItem().equals("Bag")) {
					parameters.setProcessAttributeValues(new TreeMultiSet<String>());
				} else { /* "List" */
					parameters.setProcessAttributeValues(new ArrayList<String>());
				}
			}

		});
		comboBoxCollection.setPreferredSize(new Dimension(500, 30));
		this.add(comboBoxCollection, "0, " + (y++));

		/*
		 * Add a slider to select the maximal collection size. Max-max is 100.
		 */
		this.add(new JLabel("Select collection maximal size:"), "0, " + (y++));
		final NiceSlider maxSlider = SlickerFactory.instance().createNiceIntegerSlider("Max", 0, 100,
				parameters.getMaxCollectionSize(), Orientation.HORIZONTAL);
		maxSlider.addChangeListener(new ChangeListener() {

			public void stateChanged(ChangeEvent e) {
				parameters.setMaxCollectionSize(maxSlider.getSlider().getValue());
			}
		});
		maxSlider.setPreferredSize(new Dimension(500, 30));
		this.add(maxSlider, "0, " + (y++));

		/*
		 * Update the values filter on the right-hand side.
		 */
		updateValues(log, parameters);
	}

	/*
	 * Updates the values filter on the right-hand side.
	 */
	private void updateValues(final XLog log, final AddProcessAttributeParameters parameters) {
		/*
		 * Set to hold the values.
		 */
		Set<String> values = new HashSet<String>();
		/*
		 * Collect the value for every event in every trace.
		 */
		for (XTrace trace : log) {
			for (XEvent event : trace) {
				StringBuffer buffer = new StringBuffer();
				String separator = "";
				for (String attributeKey : parameters.getAttributeKeys()) {
					XAttribute attribute = event.getAttributes().get(attributeKey);
					buffer.append(separator);
					separator = "+";
					buffer.append(attribute != null ? attribute.toString() : "");
				}
				values.add(buffer.toString());
			}
		}
		/*
		 * Copy the values in a list, and sort them. Eases look-up.
		 */
		List<String> sortedValues = new ArrayList<String>(values);
		Collections.sort(sortedValues);

		/*
		 * Add a multi-selection box for the values with all values selected.
		 * Selected values are included in the filter.
		 */
		DefaultListModel<String> valuesModel = new DefaultListModel<String>();
		int[] selectedIndices = new int[sortedValues.size()];
		int i = 0;
		for (String value : sortedValues) {
			valuesModel.add(i, value);
			selectedIndices[i] = i;
			i++;
		}
		final ProMList<String> valuesList = new ProMList<String>("Select values to include", valuesModel);
		valuesList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		valuesList.setSelectedIndices(selectedIndices);
		Set<String> selectedValues = new HashSet<String>(valuesList.getSelectedValuesList());
		parameters.setProcessAttributeValueFilter(selectedValues);
		valuesList.addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				Set<String> selectedValues = new HashSet<String>(valuesList.getSelectedValuesList());
				parameters.setProcessAttributeValueFilter(selectedValues);
			}
		});
		valuesList.setPreferredSize(new Dimension(100, 100));
		if (oldValuesList != null) {
			this.remove(oldValuesList);
		}
		this.add(valuesList, "1, 0, 1, 14");
		oldValuesList = valuesList;

		/*
		 * Wrapping up.
		 */
		this.revalidate();
		this.repaint();
	}
}
