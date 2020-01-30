package org.processmining.log.csvexport;

import java.io.File;
import java.io.IOException;

import org.deckfour.xes.model.XLog;
import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.contexts.uitopia.annotations.UIExportPlugin;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginLevel;
import org.processmining.framework.plugin.annotations.PluginVariant;

@Plugin(name = "Export Log to CSV File", level= PluginLevel.PeerReviewed, parameterLabels = { "Log", "File" }, returnLabels = {}, returnTypes = {}, userAccessible = true)
@UIExportPlugin(description = "CSV files", extension = "csv")
public final class ExportLogCsv extends ExportLogCsvAlgorithm {
	
//TODO: Export plug-in cannot show any Dialog :(
	
/*	private class DateFormatPanel extends BorderPanel {

		private static final long serialVersionUID = -6547392010448275699L;
		private final ProMTextField dateFormatTextField;

		public DateFormatPanel() {
			super(0, 0);
			dateFormatTextField = new ProMTextField("yyyy-MM-dd'T'HH:mm:ssZ");
			add(dateFormatTextField);
		}	
		
		public String getDateFormat() {
			return dateFormatTextField.getText().trim();		
		}

		public InteractionResult getUserChoice(UIPluginContext context) {
			return context.showConfiguration("Specify date format", this);
		}

	}*/
	
	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "F. Mannhardt, M. de Leoni", email = "m.d.leoni@tue.nl")
	@PluginVariant(requiredParameterLabels = { 0, 1 }, variantLabel = "Export Log to CSV File")
	public void export(UIPluginContext context, XLog log, File file) throws IOException {
		export(context, log, file, false);
	}

}
