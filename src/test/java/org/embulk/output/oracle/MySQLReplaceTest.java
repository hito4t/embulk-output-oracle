package org.embulk.output.oracle;


public class MySQLReplaceTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "src/test/resources/test-mysql-replace.yml");
	}
	
}
