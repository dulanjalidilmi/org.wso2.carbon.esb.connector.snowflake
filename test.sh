cd /Users/dulanjali/Documents/repos/connectors/org.wso2.carbon.esb.connector.snowflake;
mvn -s /Users/dulanjali/Documents/repos/settings_xml/umt_settings.xml clean install -Dmaven.test.skip=true;
cp target/snowflake-connector-1.0.0.zip /Users/dulanjali/Documents/poc/hyatt-poc/snowflake/wso2mi-4.2.0/repository/deployment/server/synapse-libs/;
#cd /Users/dulanjali/Documents/poc/hyatt-poc/snowflake/wso2mi-4.2.0/;
#./bin/micro-integrator.sh;

