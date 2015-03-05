package org.embulk.output.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.output.jdbc.JdbcColumn;
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
    
    protected boolean tableExists(String tableName) throws SQLException {
    	try (ResultSet rs = connection.getMetaData().getTables(null, schemaName, tableName, null)) {
    		return rs.next();
    	}
    }
    
    @Override
	public void dropTableIfExists(String tableName) throws SQLException
    {
    	if (tableExists(tableName)) {
            Statement stmt = connection.createStatement();
            try {
                String sql = String.format("DROP TABLE %s", quoteIdentifierString(tableName));
                executeUpdate(stmt, sql);
                commitIfNecessary(connection);
            } catch (SQLException ex) {
                throw safeRollback(connection, ex);
            } finally {
                stmt.close();
            }
    	}
    }

    @Override
	public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
    	if (!tableExists(tableName)) {
            Statement stmt = connection.createStatement();
            try {
                String sql = buildCreateTableSql(tableName, schema);
                executeUpdate(stmt, sql);
                commitIfNecessary(connection);
            } catch (SQLException ex) {
                throw safeRollback(connection, ex);
            } finally {
                stmt.close();
            }
    	}
    }

    protected String buildCreateTableSql(String name, JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        quoteIdentifierString(sb, name);
        sb.append(" (");
        boolean first = true;
        for (JdbcColumn c : schema.getColumns()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            quoteIdentifierString(sb, c.getName());
            sb.append(" ");
            String typeName = getCreateTableTypeName(c);
            sb.append(typeName);
        }
        sb.append(")");

        return sb.toString();
    }

    @Override
	public void replaceTable(String fromTable, JdbcSchema schema, String toTable) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
        	// TODO:スーパークラスも修正
        	dropTableIfExists(toTable);

            {
                StringBuilder sb = new StringBuilder();
                sb.append("ALTER TABLE ");
                quoteIdentifierString(sb, fromTable);
                sb.append(" RENAME TO ");
                quoteIdentifierString(sb, toTable);
                String sql = sb.toString();
                executeUpdate(stmt, sql);
            }

            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }
}
