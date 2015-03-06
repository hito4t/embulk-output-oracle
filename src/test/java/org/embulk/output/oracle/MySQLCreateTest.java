package org.embulk.output.oracle;


public class MySQLCreateTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "src/test/resources/test-mysql-create.yml");
	}
	
}
