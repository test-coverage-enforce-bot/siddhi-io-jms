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
import org.wso2.carbon.transport.jms.contract.JMSClientConnector;
import org.wso2.carbon.transport.jms.exception.JMSConnectorException;
import org.wso2.carbon.transport.jms.utils.JMSConstants;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * JMS publisher which creates the message and sends to JMS.
 */
public class JMSPublisher implements Runnable {
    private static final Logger log = Logger.getLogger(JMSPublisher.class);
    private Map<String, String> jmsProperties;
    private JMSClientConnector jmsClientConnector;
    private Message message;

    public JMSPublisher(String destination, Map<String, String> staticJMSProperties,
                        JMSClientConnector jmsClientConnector, Object payload)
            throws UnsupportedEncodingException {
        this.jmsProperties = new HashMap<>();
        this.jmsProperties.putAll(staticJMSProperties);
        this.jmsProperties.put(JMSConstants.PARAM_DESTINATION_NAME, destination);
        this.jmsClientConnector = jmsClientConnector;
        try {
            this.message = handleMessage(payload);
        } catch (JMSConnectorException | JMSException e) {
            log.error("Failed to process payload: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            jmsClientConnector.send(message, jmsProperties.get(JMSConstants.PARAM_DESTINATION_NAME));
        } catch (JMSConnectorException e) {
            log.error("Error sending JMS message: " + e.getMessage());
        }
    }

    private Message handleMessage(Object payload) throws UnsupportedEncodingException,
            JMSConnectorException, JMSException {
        if (payload instanceof String) {
            TextMessage message = (TextMessage) jmsClientConnector.createMessage(JMSConstants.TEXT_MESSAGE_TYPE);
            message.setText(payload.toString());
            return message;
        } else if (payload instanceof Map) {
            MapMessage message = (MapMessage) jmsClientConnector.createMessage(JMSConstants.MAP_MESSAGE_TYPE);
            ((Map) payload).forEach((key, value) -> {
                try {
                    message.setObject((String) key, value);
                } catch (JMSException e) {
                    log.error("Error while adding into message properties. " + e.getMessage());
                }
            });
            return message;
        } else if (payload instanceof ByteBuffer) {
            byte[] data = ((ByteBuffer) payload).array();
            BytesMessage message = (BytesMessage) jmsClientConnector.createMessage(JMSConstants.BYTES_MESSAGE_TYPE);
            message.writeBytes(data);
            return message;
        } else {
            throw new UnsupportedEncodingException(
                    "The type of the output payload cannot be cast to String, Map or Byte[] from JMS");
        }
    }
}
