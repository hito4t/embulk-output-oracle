package org.embulk.output.oracle;


public class InsertTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "src/test/resources/test-insert.yml");
	}
	
}
