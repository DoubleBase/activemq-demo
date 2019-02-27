package com.xiangyao.activemq.ack.mode.transacted;

import com.xiangyao.activemq.common.ActiveMQConstants;
import com.xiangyao.activemq.common.ActiveMqManager;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQPrefetchPolicy;

import javax.jms.*;
import java.util.concurrent.TimeUnit;

/**
 * @author xianggua
 * @date 2019-2-12 16:19
 * @Description
 */
public class TransactedAckConsumer {

    public static void main(String[] args) {
        ActiveMQConnection connection = null;
        try {
            connection = (ActiveMQConnection) ActiveMqManager.createConnection();
//            ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
//            prefetchPolicy.setQueuePrefetch(5);
//            connection.setPrefetchPolicy(prefetchPolicy);//预取策略
//            connection.setOptimizeAcknowledge(true);//可优化ACK,延迟确认
//            connection.setOptimizeAcknowledgeTimeOut(5000);
            connection.start();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue(ActiveMQConstants.TRANSACTED_ACK_QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            /**
             * 同步处理
             * 如果中途发生异常,那么会中断消息接收,发生异常之后的其他消息无法被消费者消费,造成消息丢失
             */
            /*while (true) {
                TextMessage textMessage = (TextMessage) consumer.receive();

                if (textMessage != null) {
                    try {
                        System.out.println("=======transacted接收消息========");
                        String text = textMessage.getText();
                        handleMsg(text);
                        if(text.contains("[2]")){
                            session.commit();
                        }
                        System.out.println("transacted成功处理消息:" + text);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }else{
                    break;
                }
            }*/

            /**
             * 异步处理
             * 如果中途发生异常,那么broker会向consumer重发消息
             * 当达到重发次数上限的时候,那么将该消息放入到DLQ队列中,继续处理其他消息;
             * 如果异常被捕获,那么不会进行重发,直接跳过处理其他消息
             */
            consumer.setMessageListener(message -> {
                System.out.println("=======transacted接收消息========");
                if (message != null) {
                    TextMessage textMessage = (TextMessage) message;
                    try {
                        String text = textMessage.getText();
                        handleMsg(text);
                        if(text.contains("[7]")||text.contains("[14]")){
                            session.commit();

                        }
                        System.out.println("transacted成功处理消息:" + text);
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
        System.out.println("transacted准备处理消息:" + msg);
        try {
            TimeUnit.SECONDS.sleep(2);
            if(msg.contains("[9]")){
                throw new RuntimeException("未知错误");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
