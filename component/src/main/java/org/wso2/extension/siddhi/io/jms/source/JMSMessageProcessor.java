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

import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.MapCarbonMessage;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.extension.siddhi.io.jms.source.exception.JMSInputAdaptorRuntimeException;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This processes the JMS messages using a pausable thread pool.
 */
public class JMSMessageProcessor implements CarbonMessageProcessor {
    private SourceEventListener sourceEventListener;
    private boolean paused;
    private ReentrantLock lock;
    private Condition condition;
    private String[] requestedTransportPropertyNames;

    public JMSMessageProcessor(SourceEventListener sourceEventListener, SiddhiAppContext
            executionPlanContext, String[] requestedTransportPropertyNames) {
        this.sourceEventListener = sourceEventListener;
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.requestedTransportPropertyNames = requestedTransportPropertyNames;
    }

    @Override
    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        if (paused) { //spurious wakeup condition is deliberately traded off for performance
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        try {
            if (carbonMessage.getClass() == TextCarbonMessage.class) {
                String[] transportProperties = populateTransportHeaders(carbonMessage);
                String event = ((TextCarbonMessage) carbonMessage).getText();
                sourceEventListener.onEvent(event, transportProperties);
            } else if (carbonMessage.getClass() == MapCarbonMessage.class) {
                String[] transportProperties = populateTransportHeaders(carbonMessage);
                Map<String, String> event = new HashMap<>();
                MapCarbonMessage mapCarbonMessage = (MapCarbonMessage) carbonMessage;
                Enumeration<String> mapNames = mapCarbonMessage.getMapNames();
                while (mapNames.hasMoreElements()) {
                    String key = mapNames.nextElement();
                    event.put(key, mapCarbonMessage.getValue(key));
                }
                sourceEventListener.onEvent(event, transportProperties);
            } else {
                throw new JMSInputAdaptorRuntimeException("The message type of the JMS message" +
                                                                  carbonMessage.getClass() + " is not supported!");
            }
            // ACK only if the event is processed i.e: no exceptions thrown from the onEvent method.
            if (carbonCallback != null) {
                carbonCallback.done(carbonMessage);
            }
        } catch (RuntimeException e) {
            throw new JMSInputAdaptorRuntimeException("Failed to process JMS message.", e);
        }
        return true;
    }

    private String[] populateTransportHeaders(CarbonMessage carbonMessage) {
        if (requestedTransportPropertyNames.length > 0) {      //cannot be null according to siddhi impl
            String[] properties = new String[requestedTransportPropertyNames.length];
            int i = 0;
            for (String property : requestedTransportPropertyNames) {
                properties[i] = carbonMessage.getHeader(property);      //can be null
                i++;
            }
            return properties;
        } else {
            return new String[0];
        }
    }

    @Override
    public void setTransportSender(TransportSender transportSender) {
    }

    @Override
    public void setClientConnector(ClientConnector clientConnector) {
    }

    @Override
    public String getId() {
        return "JMS-message-processor";
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void disconnect() {

    }
}
