package com.xiangyao.activemq.ack.mode.clientack;

import com.xiangyao.activemq.common.ActiveMQConstants;
import com.xiangyao.activemq.common.ActiveMqManager;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQPrefetchPolicy;

import javax.jms.*;
import java.util.concurrent.TimeUnit;

/**
 * @author xianggua
 * @date 2019-2-12 16:19
 * @Description 手动应答消费者
 * CLIENT_ACKNOWLEDGE：客户端手动确认模式
 * 通过 (1)message.acknowledge()
 *      (2)ActiveMQMessageConsumer.acknowledge()
 *      (3)ActiveMQSession.acknowledge()
 * 来确认消息已被处理
 * 其中(1)(3)是等效的,将当前session中所有consumer中尚未ACK的消息都一起确认
 * (2)只对当前consumer中那些尚未确认的消息进行确认
 * 为了避免混乱,建议一个session下只有一个consumer
 * 如果未进行acknowledge,那么consumer重启会接收到重复消息,因为对于broker而言,那些未进行ACK的消息会被视为“未消费”
 * 客户端确认模式，需要调用message.acknowledge()对消息确认，否则消息不能从broker中删除
 * @Version 1.0
 */
public class ClientAckConsumer {

    public static void main(String[] args) {
        ActiveMQConnection connection = null;
        try {
            connection = (ActiveMQConnection) ActiveMqManager.createConnection();
            ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
            prefetchPolicy.setQueuePrefetch(20);
            connection.setPrefetchPolicy(prefetchPolicy);//预取策略
            connection.setOptimizeAcknowledge(true);//可优化ACK,延迟确认
            connection.setOptimizeAcknowledgeTimeOut(1000);
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(ActiveMQConstants.CLIENT_ACK_QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);

            consumer.setMessageListener(message -> {
                System.out.println("=======client接收消息========");
                if (message != null) {
                    TextMessage textMessage = (TextMessage) message;
                    try {
                        String text = textMessage.getText();
                        handleMsg(text);
                        //手动确认,broker删除消息
                        textMessage.acknowledge();
                        System.out.println("client成功处理消息:" + text);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {

        }
    }

    private static void handleMsg(String msg) {
        System.out.println("client准备处理消息:" + msg);
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
