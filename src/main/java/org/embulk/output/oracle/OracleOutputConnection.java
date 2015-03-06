package org.embulk.output.oracle;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;

public class OracleOutputConnection
        extends JdbcOutputConnection
{
    public OracleOutputConnection(Connection connection, boolean autoCommit)
            throws SQLException
    {
    	// TODO:古いJDBCドライバ対応
        super(connection, connection.getSchema());
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String convertTypeName(String typeName)
    {
        switch(typeName) {
        case "BIGINT":
            return "NUMBER(19,0)";
        default:
            return typeName;
        }
    }
    
    @Override
    protected void setSearchPath(String schema) throws SQLException {
    	// NOP
    }
    
    
    @Override
	public void dropTableIfExists(String tableName) throws SQLException
    {
    	if (tableExists(tableName)) {
    		dropTable(tableName);
    	}
    }
    
    @Override
    protected void dropTableIfExists(Statement stmt, String tableName) throws SQLException {
    	if (tableExists(tableName)) {
    		dropTable(stmt, tableName);
    	}
    }

    @Override
	public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
    	if (!tableExists(tableName)) {
    		createTable(tableName, schema);
    	}
    }

}
