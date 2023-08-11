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
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.Connection;
import org.wso2.carbon.connector.core.connection.ConnectionConfig;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.pojo.ConnectionConfiguration;
import org.wso2.carbon.connector.utils.Constants;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The kafka producer connection.
 */
public class SnowflakeConnection implements Connection {
    private static Log log = LogFactory.getLog(SnowflakeConnection.class);
    private java.sql.Connection connection;

    public SnowflakeConnection(ConnectionConfiguration connectionConfiguration)
            throws SQLException, InvalidConfigurationException {
        String identifier = connectionConfiguration.getAccountIdentifier();
        String user = connectionConfiguration.getUser();
        String password = connectionConfiguration.getPassword();
        String driver = Constants.SNOWFLAKE_DRIVER;

        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(identifier, user, password);
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException(
                    String.format("Error occurred while loading the Driver: %s", driver), e);
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
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Error while closing the connection", e);
            }
        }
    }
}
