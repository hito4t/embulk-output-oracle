package org.embulk.output.oracle;

import java.sql.Connection;
import java.sql.SQLException;

import org.embulk.spi.Exec;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcColumn;
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
        case "CLOB":
            return "TEXT";
        default:
            return typeName;
        }
    }
    
    @Override
    public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException {
    	// TODO:テーブル生成には未対応
    }
    
    @Override
    protected void setSearchPath(String schema) throws SQLException {
    	// NOP
    }
}
