package org.processmining.log.plugins;

import org.deckfour.uitopia.api.event.TaskListener.InteractionResult;
import org.deckfour.xes.model.XLog;
import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.framework.plugin.annotations.PluginCategory;
import org.processmining.framework.plugin.annotations.PluginVariant;
import org.processmining.log.algorithms.AddProcessAttributeAlgorithm;
import org.processmining.log.dialogs.AddProcessAttributeDialog;
import org.processmining.log.parameters.AddProcessAttributeParameters;

@Plugin( //
		name = "Add History/Future Attribute", //
		categories = { PluginCategory.Filtering }, //
		parameterLabels = { "Event Log", "Parameters" }, //
		returnLabels = { "Filtered Log" }, //
		returnTypes = { XLog.class }, //
		userAccessible = true, //
		icon = "prom_duck_300.png", //
		url = "http://www.win.tue.nl/~hverbeek/blog/2019/12/06/add-history-future-attribute/", //
		help = "Adds a new history or future attribute to every event in the log. Click the icon on the right for additional information." //
) //
public class AddProcessAttributePlugin extends AddProcessAttributeAlgorithm {

	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "Eric Verbeek", email = "h.m.w.verbeek@tue.nl", website = "www.processmining.org", pack="Log")
	@PluginVariant(variantLabel = "Add History Attribute, Default Configuration", requiredParameterLabels = { 0 })
	public XLog runDefault(PluginContext context, XLog log) {
		return apply(log, new AddProcessAttributeParameters(log));
	}

	@PluginVariant(variantLabel = "Add History/Future Attribute, Provided Configuration", requiredParameterLabels = { 0, 1 })
	public XLog runParameters(PluginContext context, XLog log, AddProcessAttributeParameters parameters) {
		return apply(log, parameters);
	}

	@UITopiaVariant(affiliation = UITopiaVariant.EHV, author = "Eric Verbeek", email = "h.m.w.verbeek@tue.nl", website = "www.processmining.org", pack="Log")
	@PluginVariant(variantLabel = "Add History/Future Attribute, User Configuration", requiredParameterLabels = { 0 })
	public XLog runUI(UIPluginContext context, XLog log) {
		AddProcessAttributeParameters parameters = new AddProcessAttributeParameters(log);
		AddProcessAttributeDialog dialog = new AddProcessAttributeDialog(log, parameters);
		InteractionResult result = context.showWizard("Configure History/Future Attribute", true, true, dialog);
		if (result != InteractionResult.FINISHED) {
			context.getFutureResult(0).cancel(true);
			return null;
		}
		return apply(log, parameters);
	}
}
