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

import org.wso2.extension.siddhi.io.jms.source.exception.JMSInputAdaptorRuntimeException;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.transport.jms.callback.JMSCallback;
import org.wso2.transport.jms.contract.JMSListener;
import org.wso2.transport.jms.exception.JMSConnectorException;
import org.wso2.transport.jms.utils.JMSConstants;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * This processes the JMS messages using a pausable thread pool.
 */
public class JMSMessageProcessor implements JMSListener {
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
    public void onMessage(Message message, JMSCallback jmsCallback) {
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
            if (message instanceof TextMessage) {
                String[] transportProperties = populateTransportHeaders(message);
                String event = ((TextMessage) message).getText();
                sourceEventListener.onEvent(event, transportProperties);
            } else if (message instanceof MapMessage) {
                String[] transportProperties = populateTransportHeaders(message);
                Map<String, Object> event = new HashMap<>();
                MapMessage mapEvent = (MapMessage) message;
                Enumeration<String> mapNames = mapEvent.getMapNames();
                while (mapNames.hasMoreElements()) {
                    String key = mapNames.nextElement();
                    event.put(key, mapEvent.getObject(key));
                }
                sourceEventListener.onEvent(event, transportProperties);
            } else if (message instanceof ByteBuffer) {
                String[] transportProperties = populateTransportHeaders(message);
                sourceEventListener.onEvent(message, transportProperties);
            } else {
                throw new JMSInputAdaptorRuntimeException("The message type of the JMS message"
                        + message.getClass() + " is not supported!");
            }
            // ACK only if the event is processed i.e: no exceptions thrown from the onEvent method.
            if (jmsCallback != null) {
                jmsCallback.done(true);
            }
        } catch (JMSConnectorException | JMSException e) {
            throw new JMSInputAdaptorRuntimeException("Failed to process JMS message.", e);
        }
    }

    private String[] populateTransportHeaders(Message message) throws JMSException, JMSConnectorException {
        if (requestedTransportPropertyNames.length > 0) {
            //cannot be null according to siddhi impl
            message.getPropertyNames();
            String[] properties = new String[requestedTransportPropertyNames.length];
            int i = 0;
            for (String property : requestedTransportPropertyNames) {
                switch (property) {
                    case JMSConstants.JMS_REPLY_TO: {
                        if (message.getJMSReplyTo() != null) {
                            properties[i] = getDestinationName(message.getJMSReplyTo());
                            i++;
                        }
                        break;
                    }
                    case JMSConstants.JMS_DESTINATION: {
                        if (message.getJMSDestination() != null) {
                            properties[i] = getDestinationName(message.getJMSDestination());
                            i++;
                        }
                        break;
                    }
                    case JMSConstants.JMS_DELIVERY_MODE: {
                        properties[i] = String.valueOf(message.getJMSDeliveryMode());
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_CORRELATION_ID: {
                        properties[i] = message.getJMSCorrelationID();
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_EXPIRATION: {
                        properties[i] = String.valueOf(message.getJMSExpiration());
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_MESSAGE_ID: {
                        properties[i] = message.getJMSMessageID();
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_PRIORITY: {
                        properties[i] = String.valueOf(message.getJMSPriority());
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_REDELIVERED: {
                        properties[i] = String.valueOf(message.getJMSRedelivered());
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_TIMESTAMP: {
                        properties[i] = String.valueOf(message.getJMSTimestamp());
                        i++;
                        break;
                    }
                    case JMSConstants.JMS_TYPE: {
                        properties[i] = message.getJMSType();
                        i++;
                        break;
                    }
                    default: {
                        if (message.getStringProperty(property) == null) {
                            throw new JMSInputAdaptorRuntimeException(String.format("Specified property: %s is "
                                    + "not available in the message", property));
                        }
                    }
                }
            }
            return properties;
        } else {
            return new String[0];
        }
    }

    /**
     * Returns the name of the destination. The destination can be either a {@link Topic} or {@link Queue}
     * destination.
     * <p>
     * Provided {@link Destination} should be not null.
     *
     * @param jmsDestination {@link Destination}.
     * @return name of the destination.
     * @throws JMSConnectorException throws when there is an unknown {@link Destination} type is provided or
     *                               JMS error when trying to retrieve the destination name.
     */
    private String getDestinationName(Destination jmsDestination) throws JMSConnectorException {

        String destinationAsString;
        try {
            if (jmsDestination instanceof Queue) {
                destinationAsString = ((Queue) jmsDestination).getQueueName();
            } else if (jmsDestination instanceof Topic) {
                destinationAsString = ((Topic) jmsDestination).getTopicName();
            } else {
                throw new JMSConnectorException("Unknown JMS destination type. [ " + jmsDestination + " ]");
            }
            return destinationAsString;
        } catch (JMSException e) {
            throw new JMSConnectorException("Error occurred while retrieving the destination name for " +
                    "JMS Destination [ " + jmsDestination + " ]", e);
        }
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

    @Override
    public void onError(Throwable throwable) {
    }
}
