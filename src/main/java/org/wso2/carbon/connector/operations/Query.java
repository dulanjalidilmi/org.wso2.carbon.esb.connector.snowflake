package org.wso2.carbon.connector.operations;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.SnowflakeConnection;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.exception.SnowflakeOperationException;
import org.wso2.carbon.connector.utils.Constants;
import org.wso2.carbon.connector.utils.Error;
import org.wso2.carbon.connector.utils.SnowflakeUtils;

import java.sql.*;

import static java.lang.String.format;

public class Query extends AbstractConnector {

    private static final String OPERATION_NAME = "query";
    private static final String ERROR_MESSAGE = "Error occurred while performing snowflake:query operation.";

    @Override
    public void connect(MessageContext messageContext) {
        System.out.println("Querying....");
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        String connectionName = null;
        SnowflakeConnection snowflakeConnection = null;

        try {
            connectionName = SnowflakeUtils.getConnectionName(messageContext);
            snowflakeConnection = (SnowflakeConnection) handler.getConnection(Constants.CONNECTOR_NAME, connectionName);
            JsonArray jsonArray = query(messageContext, snowflakeConnection);
            SnowflakeUtils.setResultAsPayload(messageContext, jsonArray);
        } catch (InvalidConfigurationException e) {
            handleError(messageContext, e, Error.INVALID_CONFIGURATION, ERROR_MESSAGE);
        } catch (SnowflakeOperationException e) {
            handleError(messageContext, e, Error.OPERATION_ERROR, ERROR_MESSAGE);
        } catch (ConnectException e) {
            handleError(messageContext, e, Error.CONNECTION_ERROR, ERROR_MESSAGE);
        } finally {
            if (snowflakeConnection != null) {
                handler.returnConnection(Constants.CONNECTOR_NAME, connectionName, snowflakeConnection);
            }
        }
    }

    private JsonArray query(MessageContext messageContext, SnowflakeConnection snowflakeConnection)
            throws SnowflakeOperationException, InvalidConfigurationException {

        String query = (String) getParameter(messageContext, Constants.QUERY);

        if (StringUtils.isEmpty(query)) {
            throw new InvalidConfigurationException("Execute Query is not provided.");
        }

        ResultSet resultSet = null;
        Statement statement = null;

        try {
            JsonArray resultArray = new JsonArray();
            statement = snowflakeConnection.getConnection().createStatement();
            resultSet = statement.executeQuery(query);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                JsonObject result = new JsonObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String columnValue = resultSet.getString(columnName);
                    result.addProperty(columnName, columnValue);
                }
                resultArray.add(result);
            }
            return resultArray;
        } catch (SQLException e) {
            throw new SnowflakeOperationException("Error occurred while executing the query.", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                String error = format("%s:%s Error while closing the result set.",
                        Constants.CONNECTOR_NAME, OPERATION_NAME);
                log.error(error, e);
            }
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                String error = format("%s:%s Error while closing the statement.",
                        Constants.CONNECTOR_NAME, OPERATION_NAME);
                log.error(error, e);
            }

        }
    }

    private void handleError(MessageContext messageContext, Exception e, Error error, String errorDetail) {
        SnowflakeUtils.setError(OPERATION_NAME, messageContext, e, error);
        handleException(errorDetail, e, messageContext);
    }

}
