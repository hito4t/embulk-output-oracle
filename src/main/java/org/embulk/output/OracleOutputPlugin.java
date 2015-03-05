package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.oracle.OracleOutputConnector;
import org.embulk.output.oracle.setter.OracleColumnSetterFactory;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import com.google.common.base.Optional;


public class OracleOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface OraclePluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        @ConfigDefault("null")
        public Optional<String> getHost();

        @Config("port")
        @ConfigDefault("1521")
        public int getPort();

        @Config("database")
        @ConfigDefault("null")
        public Optional<String> getDatabase();
        
        @Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return OraclePluginTask.class;
    }

    @Override
    protected OracleOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        OraclePluginTask oracleTask = (OraclePluginTask) task;
        
        if (oracleTask.getDriverPath().isPresent()) {
        	// TODO:スーパークラスを利用
            // TODO:後でなんとかする。今は直JARをクラスパスに追加
            // PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
            //loader.addPath(Paths.get(oracleTask.getDriverPath().get()));
        }

        String url;
        if (oracleTask.getUrl().isPresent()) {
        	url = oracleTask.getUrl().get();
        } else {
        	if (!oracleTask.getHost().isPresent()) {
        		throw new IllegalArgumentException("Field 'host' is not set.");
        	}
        	if (!oracleTask.getDatabase().isPresent()) {
        		throw new IllegalArgumentException("Field 'database' is not set.");
        	}
        	
        	url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                    oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get());
        }

        Properties props = new Properties();
        props.setProperty("user", oracleTask.getUser());
        props.setProperty("password", oracleTask.getPassword());
        props.putAll(oracleTask.getOptions());

        return new OracleOutputConnector(url, props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        return new StandardBatchInsert(getConnector(task, true));
    }
    
    @Override
    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, PageReader pageReader,
    		TimestampFormatter timestampFormatter) {
    	return new OracleColumnSetterFactory(batch, pageReader, timestampFormatter);
    }
    
}
