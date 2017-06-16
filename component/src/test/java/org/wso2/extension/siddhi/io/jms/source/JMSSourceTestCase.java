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
package org.wso2.extension.siddhi.io.jms.source;

import org.apache.activemq.ActiveMQConnectionFactory;


import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.jms.source.client.JMSClient;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JMSSourceTestCase {
    private static final String PROVIDER_URL = "vm://localhost?broker.persistent=false";
    private List<String> receivedEventNameList;
    private int waitTime = 50;
    private int timeout = 30000;

   @Test
    public void testJMSTopicSource1() throws InterruptedException {
        AtomicInteger eventCount = new AtomicInteger(0);
        receivedEventNameList = new ArrayList<>(2);

        // starting the ActiveMQ broker
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(PROVIDER_URL);

        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@source(type='jms', @map(type='text'), "
                + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                + "provider.url='vm://localhost',"
                + "destination='DAS_JMS_TEST', "
                + "connection.factory.type='topic',"
                + "connection.factory.jndi.name='TopicConnectionFactory',"
                + "transport.jms.SubscriptionDurable='true', "
                + "transport.jms.DurableSubscriberClientID='wso2dasclient1'"
                + ")" +
                "define stream inputStream (name string, age int, country string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        executionPlanRuntime.start();

        // publishing events
        publishEvents("DAS_JMS_TEST", null, "activemq", "text", "src/test/resources/events/events_text.txt");
        List<String> expected = new ArrayList<>(2);
        expected.add("\nJohn");
        expected.add("\nMike");
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 2, "Event count should be equal to two.");
        AssertJUnit.assertEquals("JMS Source expected input not received", expected, receivedEventNameList);
        siddhiManager.shutdown();
    }

    @Test
    public void testJMSTopicSource2() throws InterruptedException {
        AtomicInteger eventCount = new AtomicInteger(0);
        receivedEventNameList = new ArrayList<>(2);

        // starting the ActiveMQ broker
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(PROVIDER_URL);

        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "" +
                "@source(type='jms', @map(type='xml'), "
                + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
                + "provider.url='vm://localhost',"
                + "destination='DAS_JMS_TEST', "
                + "connection.factory.type='topic',"
                + "connection.factory.jndi.name='TopicConnectionFactory',"
                + "transport.jms.SubscriptionDurable='true', "
                + "transport.jms.DurableSubscriberClientID='wso2dasclient1'"
                + ")" +
                "define stream inputStream (name string, age int, country string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                                                                                                     query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        executionPlanRuntime.start();
        List<String> messageList = new ArrayList<>(2);
        messageList.add("<events>\n"
                        + "    <event>\n"
                        + "        <name>John</name>\n"
                        + "        <age>22</age>\n"
                        + "        <country>US</country>\n"
                        + "    </event>\n"
                        + "    <event>\n"
                        + "        <name>Mike</name>\n"
                        + "        <age>24</age>\n"
                        + "        <country>US</country>\n"
                        + "    </event>\n"
                        + "</events>");
        // publishing events
        publishEvents("DAS_JMS_TEST", null, "activemq", "text", messageList);

        List<String> expected = new ArrayList<>(2);
        expected.add("John");
        expected.add("Mike");
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertEquals("JMS Source expected input not received", expected, receivedEventNameList);
        siddhiManager.shutdown();
    }

    //Commenting out till json is released
//    @Test
//    public void testJMSTopicSource3() throws InterruptedException {
//        AtomicInteger eventCount = new AtomicInteger(0);
//        receivedEventNameList = new ArrayList<>(2);
//
//        // starting the ActiveMQ broker
//        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(PROVIDER_URL);
//
//        // deploying the execution plan
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String inStreamDefinition = "" +
//                "@source(type='jms', @map(type='json'), "
//                + "factory.initial='org.apache.activemq.jndi.ActiveMQInitialContextFactory', "
//                + "provider.url='vm://localhost',"
//                + "destination='DAS_JMS_TEST', "
//                + "connection.factory.type='queue',"
//                + "connection.factory.jndi.name='QueueConnectionFactory'"
//                + ")" +
//                "define stream inputStream (name string, age int, country string);";
//        String query = ("@info(name = 'query1') " +
//                "from inputStream " +
//                "select *  " +
//                "insert into outputStream;");
//        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
//                                                                                                     query);
//
//        executionPlanRuntime.addCallback("query1", new QueryCallback() {
//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                for (Event event : inEvents) {
//                    eventCount.incrementAndGet();
//                    receivedEventNameList.add(event.getData(0).toString());
//                }
//            }
//        });
//        executionPlanRuntime.start();
//        List<String> messageList = new ArrayList<>(2);
//        messageList.add("[ {\"event\":{\"name\":\"John\",\"age\":50,\"country\":\"US\"}},\n"
//                                + " {\"event\":{\"name\":\"Mike\",\"age\":23,\"country\":\"US\"}}\n"
//                                + "]");
//        // publishing events
//        publishEvents(null, "DAS_JMS_TEST", "activemq", "text", messageList);
//
//        List<String> expected = new ArrayList<>(2);
//        expected.add("\"John\"");   //todo fix after json mapper bug is fixed
//        expected.add("\"Mike\"");
//        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
//        Assert.assertEquals(receivedEventNameList, expected, "JMS Source expected input not received");
//        siddhiManager.shutdown();
//    }


    private void publishEvents(String topicName, String queueName, String broker, String format, String filePath)
            throws InterruptedException {
        JMSClient jmsClient = new JMSClient();
        jmsClient.sendJMSEvents(filePath, topicName, queueName, format, broker, PROVIDER_URL);
    }

    private void publishEvents(String topicName, String queueName, String broker, String format, List<String>
            messageList)
            throws InterruptedException {
        JMSClient jmsClient = new JMSClient();
        jmsClient.sendJMSEvents(messageList, topicName, queueName, format, broker, PROVIDER_URL);
    }
}
