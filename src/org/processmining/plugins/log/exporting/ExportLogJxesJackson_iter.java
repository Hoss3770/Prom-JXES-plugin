package org.processmining.plugins.log.exporting;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.deckfour.xes.model.XLog;
import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.contexts.uitopia.annotations.UIExportPlugin;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginLevel;
import org.processmining.framework.plugin.annotations.PluginVariant;

@Plugin(name = "Export Log to JXES File (Jackson iter)", level = PluginLevel.PeerReviewed, parameterLabels = { "Log", "File" }, returnLabels = {}, returnTypes = {}, userAccessible = true)
@UIExportPlugin(description = "JXES files (Jackson iter)", extension = "json")
public class ExportLogJxesJackson_iter extends AbstractLogExporter {

	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "Hossameldin Khalifa", email = "hosskhalifa@gmail.com")
	@PluginVariant(requiredParameterLabels = { 0, 1 }, variantLabel = "Export Log to JXES File (Jackson iter)")
	public void export(UIPluginContext context, XLog log, File file) throws IOException {
		exportWithNameFromContext(context, log, file);
	}

	void doExport(XLog log, File file) throws IOException {
		// Calls the static method for backwards compatibility
		export(log, file);
	}

	public static void export(XLog log, File file) throws IOException {
		FileOutputStream out = new FileOutputStream(file);
		JxesJacksonSerializer_iter logSerializer = new JxesJacksonSerializer_iter();
		logSerializer.serialize(log, out);
		out.close();
	}

}