/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.jms.sink;

import org.apache.log4j.Logger;
import org.wso2.carbon.transport.jms.sender.JMSClientConnector;
import org.wso2.extension.siddhi.io.jms.util.JMSOptionsMapper;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static org.wso2.extension.siddhi.io.jms.util.JMSOptionsMapper.DESTINATION;

/**
 * JMS output transport class.
 * Dynamic options: destination
 */
@Extension(
        name = "jms",
        namespace = "sink",
        description = "JMS Sink allows users to subscribe to a JMS broker and publish JMS messages.",
        parameters = {
                @Parameter(name = JMSOptionsMapper.DESTINATION,
                           description = "Queue/Topic name which JMS Source should subscribe to",
                           type = DataType.STRING,
                           dynamic = true
                ),
                @Parameter(name = JMSOptionsMapper.CONNECTION_FACTORY_JNDI_NAME,
                           description = "JMS Connection Factory JNDI name. This value will be used for the JNDI "
                                   + "lookup to find the JMS Connection Factory.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "QueueConnectionFactory"),
                @Parameter(name = JMSOptionsMapper.FACTORY_INITIAL,
                           description = "Naming factory initial value",
                           type = DataType.STRING),
                @Parameter(name = JMSOptionsMapper.PROVIDER_URL,
                           description = "Java naming provider URL. Property for specifying configuration "
                                   + "information for the service provider to use. The value of the property should "
                                   + "contain a URL string (e.g. \"ldap://somehost:389\")",
                           type = DataType.STRING),
                @Parameter(name = JMSOptionsMapper.CONNECTION_FACTORY_TYPE,
                           description = "Type of the connection connection factory. This can be either queue or "
                                   + "topic.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "queue"),
                @Parameter(name = JMSOptionsMapper.CONNECTION_USERNAME,
                           description = "username for the broker.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "None"),
                @Parameter(name = JMSOptionsMapper.CONNECTION_PASSWORD,
                           description = "Password for the broker",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "None"),
                @Parameter(name = JMSOptionsMapper.CONNECTION_FACTORY_NATURE,
                           description = "Connection factory nature for the broker(cached/pooled).",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "default")
        },
        examples = {
                @Example(description = "Following example illustrates how to publish to an ActiveMQ topic.",
                         syntax = "@sink(type='jms', @map(type='xml'), "
                                 + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                                 + "provider.url='vm://localhost',"
                                 + "destination='DAS_JMS_OUTPUT_TEST', "
                                 + "connection.factory.type='topic',"
                                 + "connection.factory.jndi.name='TopicConnectionFactory'"
                                 + ")" +
                                 "define stream inputStream (name string, age int, country string);"),
                @Example(description = "Following example illustrates how to publish to an ActiveMQ queue. "
                        + "Note that we are not providing properties like connection factory type",
                         syntax = "@sink(type='jms', @map(type='xml'), "
                                 + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                                 + "provider.url='vm://localhost',"
                                 + "destination='DAS_JMS_OUTPUT_TEST')" +
                                 "define stream inputStream (name string, age int, country string);")
        }
)
public class JMSSink extends Sink {
    private static final Logger log = Logger.getLogger(JMSSink.class);
    private OptionHolder optionHolder;
    private JMSClientConnector clientConnector;
    private Option destination;
    private Map<String, String> jmsStaticProperties;
    private ExecutorService executorService;

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader sinkConfigReader, SiddhiAppContext executionPlanContext) {
        this.optionHolder = optionHolder;
        this.destination = optionHolder.getOrCreateOption(DESTINATION, null);
        this.jmsStaticProperties = initJMSProperties();
        this.executorService = executionPlanContext.getExecutorService();
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        this.clientConnector = new JMSClientConnector();
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions) {
        String topicQueueName = destination.getValue(transportOptions);
        try {
            executorService.submit(new JMSPublisher(topicQueueName, jmsStaticProperties, clientConnector, payload));
        } catch (RejectedExecutionException e) {
            //No need to retry as we are using an unbounded queue. Only place this can happen is when the executor
            // service is shutting down. In such cases we can't anyway handle the exception. Hence logging.
            log.error("Error occured when submitting following payload to be published via JMS. Payload : " + payload
                    .toString(), e);
        }
    }

    @Override public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Map.class, Byte[].class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{DESTINATION};
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void destroy() {

    }

    /**
     * Initializing JMS properties.
     * The properties in the required options list are mandatory.
     * Other JMS options can be passed in as key value pairs, key being in the JMS spec or the broker spec.
     *
     * @return all the options map.
     */
    private Map<String, String> initJMSProperties() {
        List<String> requiredOptions = JMSOptionsMapper.getRequiredOptions();
        Map<String, String> customPropertyMapping = JMSOptionsMapper.getCarbonPropertyMapping();
        // getting the required values
        Map<String, String> transportProperties = new HashMap<>();
        requiredOptions.forEach(requiredOption ->
                transportProperties.put(customPropertyMapping.get(requiredOption),
                        optionHolder.validateAndGetStaticValue(requiredOption)));
        // getting optional values
        optionHolder.getStaticOptionsKeys().stream()
                .filter(option -> !requiredOptions.contains(option) && !option.equals("type")).forEach(option ->
                transportProperties.put(customPropertyMapping.get(option), optionHolder.validateAndGetStaticValue
                        (option)));
        return transportProperties;
    }
}
