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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.jms.sink.util.JMSClient;
import org.wso2.extension.siddhi.io.jms.sink.util.ResultContainer;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class JMSSinkTestCase {



    @Test
    public void jmsTopicPublishTest() throws InterruptedException {
        SiddhiAppRuntime executionPlanRuntime = null;
        ResultContainer resultContainer = new ResultContainer(2);
        JMSClient client = new JMSClient("activemq", "", "DAS_JMS_OUTPUT_TEST", resultContainer);
        try {
            //init
            Thread listenerThread = new Thread(client);
            listenerThread.start();

            // deploying the execution plan
            SiddhiManager siddhiManager = new SiddhiManager();
            String inStreamDefinition = "" +
                    "@sink(type='jms', @map(type='xml'), "
                    + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                    + "provider.url='vm://localhost',"
                    + "destination='DAS_JMS_OUTPUT_TEST', "
                    + "connection.factory.type='queue',"
                    + "connection.factory.jndi.name='QueueConnectionFactory'"
                    + ")" +
                    "define stream inputStream (name string, age int, country string);";
            executionPlanRuntime = siddhiManager.
                    createSiddhiAppRuntime(inStreamDefinition);
            InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
            executionPlanRuntime.start();
            inputStream.send(new Object[]{"JAMES", 23, "USA"});
            inputStream.send(new Object[]{"MIKE", 23, "Germany"});

            Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
            Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        } finally {
            client.shutdown();
            if (executionPlanRuntime != null) {
                executionPlanRuntime.shutdown();
            }
        }
    }
}
