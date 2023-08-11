package org.wso2.carbon.connector.exception;

/**
 * Exception thrown when necessary parameters are not configured
 */
public class InvalidConfigurationException extends Exception {

    public InvalidConfigurationException(String message, Throwable cause) {

        super(message, cause);
    }

    public InvalidConfigurationException(String message) {

        super(message);


    }
}
