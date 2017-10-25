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
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.MapCarbonMessage;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.transport.jms.sender.JMSClientConnector;
import org.wso2.carbon.transport.jms.utils.JMSConstants;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * JMS publisher which creates the message and sends to JMS.
 */
public class JMSPublisher implements Runnable {
    private static final Logger log = Logger.getLogger(JMSPublisher.class);

    // this field has to be set for the TextCarbonMessage for it to work
    private static final String MESSAGE_TYPE_FIELD = "JMS_MESSAGE_TYPE";
    private static final String TEXT_MESSAGE_TYPE = "TextMessage";
    private static final String BYTE_ARRAY_MESSAGE_TYPE = "ByteMessage";

    private Map<String, String> jmsProperties;
    private JMSClientConnector jmsClientConnector;
    private CarbonMessage message;

    public JMSPublisher(String destination, Map<String, String> staticJMSProperties,
            JMSClientConnector jmsClientConnector, Object payload) throws UnsupportedEncodingException {
        this.jmsProperties = new HashMap<>();
        this.jmsProperties.putAll(staticJMSProperties);
        this.jmsProperties.put(JMSConstants.PARAM_DESTINATION_NAME, destination);
        this.jmsClientConnector = jmsClientConnector;
        this.message = handleCarbonMessage(payload);
    }

    @Override
    public void run() {
        try {
            jmsClientConnector.send(message, null, jmsProperties);
        } catch (ClientConnectorException e) {
            log.error("Error sending JMS message: ", e);
        }
    }

    private CarbonMessage handleCarbonMessage(Object payload) throws UnsupportedEncodingException {

        if (payload instanceof String) {
            CarbonMessage textCarbonMessage = new TextCarbonMessage(payload.toString());
            this.jmsProperties.put(MESSAGE_TYPE_FIELD, TEXT_MESSAGE_TYPE);
            return textCarbonMessage;
        } else if (payload instanceof Map) {
            MapCarbonMessage mapCarbonMessage = new MapCarbonMessage();
            ((Map) payload).forEach((key, value) -> {
                mapCarbonMessage.setValue((String) key, (String) value);
            });
            return mapCarbonMessage;
        } else if (payload instanceof ByteBuffer) {
            byte[]  data = ((ByteBuffer) payload).array();
            String text = new String(data, "UTF-8");
            TextCarbonMessage byteCarbonMessage = new TextCarbonMessage(text);
            this.jmsProperties.put(MESSAGE_TYPE_FIELD, BYTE_ARRAY_MESSAGE_TYPE);
            log.info(byteCarbonMessage.getMessageBody());
            return byteCarbonMessage;

        } else {
            throw new RuntimeException(
                    "The type of the output payload cannot be cast to String, Map or Byte[] from JMS");
        }

    }
}
