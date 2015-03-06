package org.embulk.output.oracle;


public class LongNameTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "src/test/resources/test-long-name.yml");
	}
	
}
