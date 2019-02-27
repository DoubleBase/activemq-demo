package com.xiangyao.activemq.common;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * @author xianggua
 * @date 2019-2-12 16:23
 * @Description
 * @Version 1.0
 */
public class ActiveMqManager {

    private static ConnectionFactory connectionFactory;

    private static final String brokerUrl = "tcp://127.0.0.1:61616";

    static {
        connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    }

    public static Connection createConnection() throws JMSException {
        return connectionFactory.createConnection();
    }

}
