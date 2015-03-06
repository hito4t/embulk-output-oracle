package org.embulk.output.oracle;


public class ReplaceTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "src/test/resources/test-replace.yml");
	}
	
}
