package org.embulk.output.oracle;

import org.embulk.cli.Main;

public class ReplaceTest {
	
	public static void main(String[] args) {
		Main.main(new String[]{"run", "src/test/resources/test-replace.yml"});
	}
	
}
