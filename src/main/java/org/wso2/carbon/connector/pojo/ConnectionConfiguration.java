package org.wso2.carbon.connector.pojo;

import org.apache.commons.lang3.StringUtils;
import org.wso2.carbon.connector.core.pool.Configuration;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;

public class ConnectionConfiguration {

    private String connectionName;
    private String accountIdentifier;
    private String user;
    private String password;

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getAccountIdentifier() {
        return accountIdentifier;
    }

    public void setAccountIdentifier(String accountIdentifier) throws InvalidConfigurationException {
        if (StringUtils.isEmpty(accountIdentifier)) {
            throw new InvalidConfigurationException("Mandatory parameter 'accountIdentifier' is not set.");
        }
        this.accountIdentifier = "jdbc:snowflake://".concat(accountIdentifier).concat(".snowflakecomputing.com/");
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) throws InvalidConfigurationException {
        if (StringUtils.isEmpty(user)) {
            throw new InvalidConfigurationException("Mandatory parameter 'user' is not set.");
        }
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) throws InvalidConfigurationException {
        if (StringUtils.isEmpty(password)) {
            throw new InvalidConfigurationException("Mandatory parameter 'password' is not set.");
        }
        this.password = password;
    }
}
