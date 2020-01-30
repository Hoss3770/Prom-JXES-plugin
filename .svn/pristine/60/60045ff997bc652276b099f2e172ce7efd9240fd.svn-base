package org.processmining.tests.log;
import junit.framework.TestCase;

import org.junit.Test;
import org.processmining.contexts.cli.CLI;

public class LogTest extends TestCase {

  @Test
  public void testLog1() throws Throwable {
    String args[] = new String[] {"-l"};
    CLI.main(args);
  }

  @Test
  public void testLog2() throws Throwable {
    String testFileRoot = System.getProperty("test.testFileRoot", ".");
    String args[] = new String[] {"-f", testFileRoot+"/Log_Example.txt"};
    
    CLI.main(args);
  }
  
  public static void main(String[] args) {
    junit.textui.TestRunner.run(LogTest.class);
  }
  
}
