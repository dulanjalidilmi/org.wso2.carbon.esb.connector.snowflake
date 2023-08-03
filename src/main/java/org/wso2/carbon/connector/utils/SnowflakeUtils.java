package org.wso2.carbon.connector.utils;

import org.apache.synapse.MessageContext;

public class SnowflakeUtils {
    public static String getConnectionName(MessageContext messageContext) {
        String connectionName = (String) messageContext.getProperty(Constants.CONNECTION_NAME);
        if (connectionName == null) {
            // todo handle this
            throw new RuntimeException("Connection name is not set.");
        }
        return connectionName;
    }
}
