package org.processmining.log.utils;

import static org.junit.Assert.assertEquals;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.junit.Test;

public class XLogBuilderTest {
	
	@Test
	public void testLogBuilder() {
		
		XLog log = XLogBuilder.newInstance()
				.startLog("test")
				.addTrace("t1", 2)
					.addEvent("A")
					.addAttribute("X", 21)
					.addEvent("B")
					.addEvent("D")
					.addEvent("E", 2)
				.addTrace("t2")
					.addEvent("A", 2)
					.addAttribute("X", 20)
					.addEvent("D")
					.addEvent("E")
				.addTrace("t3", 2)
					.addEvent("A")
					.addAttribute("X", 19)
					.addEvent("C")
					.addEvent("D")
					.addEvent("E")
				.build();
		
		assertEquals(5, log.size());
		assertEquals("test", XConceptExtension.instance().extractName(log));
		
		XTrace t1 = log.get(0);
		assertEquals(5, t1.size());
		assertEquals("E", XConceptExtension.instance().extractName(t1.get(4)));
		assertEquals("A", XConceptExtension.instance().extractName(t1.get(0)));
		assertEquals(21, ((XAttributeDiscrete)t1.get(0).getAttributes().get("X")).getValue());
		
		XTrace t3 = log.get(3);
		assertEquals(4, t3.size());
		assertEquals("t3", XConceptExtension.instance().extractName(t3));
		
		XTrace t31 = log.get(4);
		assertEquals(4, t31.size());
		assertEquals("t3-1", XConceptExtension.instance().extractName(t31));
		
	}

}
