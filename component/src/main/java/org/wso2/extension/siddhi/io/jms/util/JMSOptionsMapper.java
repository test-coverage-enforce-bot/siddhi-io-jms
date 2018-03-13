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
package org.wso2.extension.siddhi.io.jms.util;

import org.wso2.transport.jms.utils.JMSConstants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Used to do the custom mapping from the siddhi extension to JMS Carbon Transport.
 */
public class JMSOptionsMapper {
    public static final String DESTINATION = "destination";
    public static final String CONNECTION_FACTORY_JNDI_NAME = "connection.factory.jndi.name";
    public static final String FACTORY_INITIAL = "factory.initial";
    public static final String PROVIDER_URL = "provider.url";
    public static final String CONNECTION_FACTORY_TYPE = "connection.factory.type";

    public static final String WORKER_COUNT = "worker.count";
    public static final String CONNECTION_USERNAME = "connection.username";
    public static final String CONNECTION_PASSWORD = "connection.password";
    public static final String RETRY_INTERVAL = "retry.interval";
    public static final String MAX_RETRY_COUNT = "retry.count";
    public static final String USE_RECEIVER = "use.receiver";
    public static final String PARAM_SUB_DURABLE = "subscription.durable";
    public static final String CONNECTION_FACTORY_NATURE = "connection.factory.nature";
    /**
     * Returns the custom property map mapping the siddhi extension key name to JMS transport key.
     *
     * @return map of custom mapping
     */
    public static Map<String, String> getCarbonPropertyMapping() {
        Map<String, String> carbonPropertyMapping = new HashMap<>();
        carbonPropertyMapping.put(DESTINATION, JMSConstants.PARAM_DESTINATION_NAME);
        carbonPropertyMapping.put(CONNECTION_FACTORY_JNDI_NAME, JMSConstants.PARAM_CONNECTION_FACTORY_JNDI_NAME);
        carbonPropertyMapping.put(FACTORY_INITIAL, JMSConstants.PARAM_NAMING_FACTORY_INITIAL);
        carbonPropertyMapping.put(PROVIDER_URL, JMSConstants.PARAM_PROVIDER_URL);
        carbonPropertyMapping.put(CONNECTION_FACTORY_TYPE, JMSConstants.PARAM_CONNECTION_FACTORY_TYPE);
        carbonPropertyMapping.put(WORKER_COUNT, JMSConstants.CONCURRENT_CONSUMERS);
        carbonPropertyMapping.put(CONNECTION_USERNAME, JMSConstants.CONNECTION_USERNAME);
        carbonPropertyMapping.put(CONNECTION_PASSWORD, JMSConstants.CONNECTION_PASSWORD);
        carbonPropertyMapping.put(RETRY_INTERVAL, JMSConstants.RETRY_INTERVAL);
        carbonPropertyMapping.put(MAX_RETRY_COUNT, JMSConstants.MAX_RETRY_COUNT);
        carbonPropertyMapping.put(USE_RECEIVER, JMSConstants.USE_RECEIVER);
        carbonPropertyMapping.put(PARAM_SUB_DURABLE, JMSConstants.PARAM_SUB_DURABLE);
        carbonPropertyMapping.put(CONNECTION_FACTORY_NATURE, JMSConstants.CONNECTION_FACTORY_NATURE);
        return carbonPropertyMapping;
    }

    /**
     * Returns the required options for the JMS input transport.
     *
     * @return list of required options.
     */
    public static List<String> getRequiredOptions() {
        return Arrays.asList(DESTINATION, FACTORY_INITIAL, PROVIDER_URL);
    }
}
