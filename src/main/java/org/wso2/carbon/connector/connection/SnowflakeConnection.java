/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.connector.connection;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.Connection;
import org.wso2.carbon.connector.core.connection.ConnectionConfig;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.utils.Constants;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The kafka producer connection.
 */
public class SnowflakeConnection implements Connection {
    private static Log log = LogFactory.getLog(SnowflakeConnection.class);
    private java.sql.Connection connection;

    public SnowflakeConnection(MessageContext messageContext){
//        Axis2MessageContext axis2mc = (Axis2MessageContext) messageContext;
        String identifier = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.ACCOUNT_IDENTIFIER);
        String user = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.USER);
        String password = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.PASSWORD);
        log.info("identifier: " + identifier);
        log.info("user: " + user);
        log.info("password: " + password);
        String driver = "net.snowflake.client.jdbc.SnowflakeDriver";

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(identifier, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public java.sql.Connection getConnection(){
        return this.connection;
    }

    @Override
    public void connect(ConnectionConfig connectionConfig) throws ConnectException {
        //no requirement to implement for now
    }

    @Override
    public void close() throws ConnectException {
        //no requirement to implement for now
    }

    public void returnConnection() {
        //no requirement to implement for now
    }
}
