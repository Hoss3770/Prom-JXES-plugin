package org.processmining.log.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XLog;
import org.junit.Test;

public class XUtilsTest {

	@Test
	public void testConceptName() {
		XLog log = XLogBuilder.newInstance().startLog("test").addTrace("trace").addEvent("event").build();
		assertEquals("test", XUtils.getConceptName(log));
		assertEquals("trace", XUtils.getConceptName(log.get(0)));
		assertEquals("event", XUtils.getConceptName(log.get(0).get(0)));
	}

	@Test
	public void testTraceVariants() {
		XLog log = XLogBuilder.newInstance().startLog("log").addTrace("1", 10).addEvent("a").addEvent("b").addEvent("c")
				.addTrace("2", 5).addEvent("a").build();

		assertEquals(2, XUtils.countVariantsByClassifier(log, new XEventNameClassifier()));
		
		assertEquals(2, XUtils.getVariantsByClassifier(log, new XEventNameClassifier()).keySet().size());
	}
	
	@Test
	public void testSameType() {

		XAttribute a = XUtils.createAttribute("a", "a");
		XAttribute b = XUtils.createAttribute("b", "b");

		XAttribute c = XUtils.createAttribute("c", false);
		XAttribute d = XUtils.createAttribute("c", true);
		
		XAttribute e = XUtils.createAttribute("c", 2);
		XAttribute f = XUtils.createAttribute("c", 3);
		
		assertTrue(XUtils.isSameType(a, b));
		assertFalse(XUtils.isSameType(a, c));
		assertTrue(XUtils.isSameType(c, d));
		
		assertTrue(XUtils.isSameType(e, f));
		assertFalse(XUtils.isSameType(e, c));
		assertFalse(XUtils.isSameType(e, b));
		
	}

}
