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

@Plugin(name = "Export Log to Sparse CSV File", level= PluginLevel.PeerReviewed, parameterLabels = { "Log", "File" }, returnLabels = {}, returnTypes = {}, userAccessible = true)
@UIExportPlugin(description = "Sparse CSV files", extension = "csv")
public final class ExportLogCsvSparse extends ExportLogCsvAlgorithm {
	
	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "F. Mannhardt, M. de Leoni", email = "m.d.leoni@tue.nl")
	@PluginVariant(requiredParameterLabels = { 0, 1 }, variantLabel = "Export Log to Sparse CSV File")
	public void exportSparse(UIPluginContext context, XLog log, File file) throws IOException {
		export(context, log, file, true);
	}
	
}
