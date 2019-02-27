package com.xiangyao.activemq.ack.mode.dupsokack;

import com.xiangyao.activemq.common.ActiveMQConstants;
import com.xiangyao.activemq.common.ActiveMqManager;

import javax.jms.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author xianggua
 * @date 2019-2-12 16:27
 * @Description
 * @Version 1.0
 */
public class DupsOkAckSender {

    private static Connection connection;

    static {
        try {
            connection = ActiveMqManager.createConnection();
            connection.start();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private static void sendMessage(String message) {
        Session session = null;
        MessageProducer messageProducer = null;
        try {
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue queue = session.createQueue(ActiveMQConstants.DUPS_OK_ACK_QUEUE_NAME);
            messageProducer = session.createProducer(queue);
            TextMessage textMessage = session.createTextMessage(message);
            messageProducer.send(textMessage);
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                if (messageProducer != null) {
                    messageProducer.close();
                }
                if (session != null) {
                    session.close();
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        DupsOkAckSender autoAckSender = new DupsOkAckSender();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < 15; i++) {
            String date = simpleDateFormat.format(new Date());
            String message = String.format("msg[%s][%s]",i,date);
            DupsOkAckSender.sendMessage(message);
            System.out.println("dups成功发送消息："+message);
            try {
                //1秒发送1条
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        autoAckSender.close();
    }

    private void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

}
