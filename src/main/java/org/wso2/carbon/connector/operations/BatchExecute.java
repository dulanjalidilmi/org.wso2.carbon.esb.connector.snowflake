package org.wso2.carbon.connector.operations;

import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.connector.connection.SnowflakeConnection;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.pojo.SnowflakesOperationResult;
import org.wso2.carbon.connector.utils.Constants;
import org.wso2.carbon.connector.utils.SnowflakeUtils;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BatchExecute extends AbstractConnector {
    private static final String OPERATION_NAME = "batchExecute";
    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        String connectionName = SnowflakeUtils.getConnectionName(messageContext);
        SnowflakeConnection snowflakeConnection = (SnowflakeConnection) handler.getConnection(Constants.CONNECTOR_NAME,
                connectionName);

        batchExecuteQuery(messageContext, snowflakeConnection);
    }

    private void batchExecuteQuery(MessageContext messageContext, SnowflakeConnection snowflakeConnection) {
        SnowflakesOperationResult snowflakesOperationResult = new SnowflakesOperationResult(OPERATION_NAME, false);
        String query = (String) getParameter(messageContext, Constants.EXECUTE_QUERY);
        String payload = (String) getParameter(messageContext, Constants.PAYLOAD);

        if(query == null || query.isEmpty()) {
            // todo throw exception
            log.error("Query is empty");
            return;
        }
        if(payload == null) {
            // todo throw exception
            log.error("Payload is empty");
            return;
        }
        JSONArray jsonArray = null;

        try {
            jsonArray = new JSONArray(payload);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String[] columns = getColumnNames(query);

        try {
            PreparedStatement preparedStatement = snowflakeConnection.getConnection().prepareStatement(query);
            if (jsonArray.length()>0 && columns.length>0) {
                for (int i = 0; i<jsonArray.length(); i++) {
                    JSONObject data = jsonArray.getJSONObject(i);
                    int increment = 0;
                    for (String column : columns) {
                        Object value = data.get(column);
                        preparedStatement.setString(++increment, (value != null) ? value.toString() : "");
                    }
                    preparedStatement.addBatch();
                }
                int[] batchResult = preparedStatement.executeBatch();
                if (batchResult.length > 0) {
                    snowflakesOperationResult = new SnowflakesOperationResult(OPERATION_NAME, true);
                    SnowflakeUtils.setResultAsPayload(messageContext, snowflakesOperationResult);
                }
            } else {
//                todo
            }
//            JsonUtil.getJsonPayload(messageContext.setProperty("results", snowflakesOperationResult));
            preparedStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String[] getColumnNames(String query) {
        // Using regular expression to match the column names inside the parentheses
        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);
        String[] columnNames = null;

        if (matcher.find()) {
            String columnNamesStr = matcher.group(1);
            columnNames = columnNamesStr.split("\\s*,\\s*");
        }
        return columnNames;
    }
}
