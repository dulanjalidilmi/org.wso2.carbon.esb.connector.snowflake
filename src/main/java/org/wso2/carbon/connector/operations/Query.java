package org.wso2.carbon.connector.operations;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.SnowflakeConnection;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.utils.Constants;
import org.wso2.carbon.connector.utils.SnowflakeUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Query extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        System.out.println("Querying....");
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        String connectionName = SnowflakeUtils.getConnectionName(messageContext);
        SnowflakeConnection snowflakeConnection = (SnowflakeConnection) handler.getConnection(Constants.CONNECTOR_NAME,
                connectionName);

        executeQuery(messageContext, snowflakeConnection);
    }

    private void executeQuery(MessageContext messageContext, SnowflakeConnection snowflakeConnection) {
        String query = (String) getParameter(messageContext, Constants.QUERY);
        if(query == null || query.isEmpty()) {
            // todo throw exception
            log.error("Query is empty");
            return;
        }
        ResultSet resultSet = null;
        Statement statement = null;

        try {
            statement = snowflakeConnection.getConnection().createStatement();
            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                // Process the data from the result set
                String LASTNAME = resultSet.getString("LASTNAME");
                String FIRSTNAME = resultSet.getString("FIRSTNAME");
                String COMPANY = resultSet.getString("COMPANY");
                // Add more columns as needed
                System.out.println("Full details of employee: {\"LASTNAME\":" + LASTNAME + ",\"FIRSTNAME\":" + FIRSTNAME + ",\"COMPANY\":" + COMPANY + "}");
            }

            // Close the connections and resources

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                resultSet.close();
                statement.close();
                snowflakeConnection.returnConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }


}
