package edu.cibertec.jaad.jms;

import org.apache.log4j.Logger;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.swing.*;
import java.awt.*;
import java.util.UUID;

/**
 * @author Carlos Larico [Stuk4] cdaniel.lf@gmail.com
 */
public class JMSClient {

    public static final Logger LOG = Logger.getLogger(JMSClient.class);
    public static final long WAITING_MSG = 601;

    public static final String JMS_CONN_FAC = "jms/QUEUECF";
    public static final String JMS_QUEUE_IN = "jms/QueueIn";
    public static final String JMS_QUEUE_OUT = "jms/QueueOut";

    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private Destination queueIn;
    private Destination queueOut;
    private Context ctx;

    private MessageProducer producer;
    private MessageConsumer consumer;

    /**
     * execute
     */
    public void execute(){
        createConnectionAndSession();
        createDestination();
        createProducerAndConsumer();
        processMessage();
        closeResources();
    }

    /**
     * Process Message
     */
    private void processMessage(){
        try {
            //send message
            MapMessage msgReq = session.createMapMessage();
            msgReq.setString("OPERACION","Recarga");
            msgReq.setDouble("MONTO", 35.0);
            String correlationId = UUID.randomUUID().toString();
            msgReq.setJMSCorrelationID(correlationId);
            producer.send(msgReq);

            LOG.info("Esperando por respuesta " + WAITING_MSG  + "seg");
            // recibiendo msg
            TextMessage msgRes = (TextMessage) consumer.receive(WAITING_MSG);

            LOG.info("Mensaje recibido " + msgRes);
            String result = msgRes == null? "SIN RESPUESTA" : msgRes.getText();

            LOG.info("Result: " +result);



        } catch (JMSException e) {
            LOG.error("Error in process message " , e);
        }
    }

    /**
     * jms lookUp
     */
    public void createConnectionAndSession(){
        try {
            ctx = new InitialContext();
            factory = (ConnectionFactory) ctx.lookup(JMS_CONN_FAC);
            connection = factory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (NamingException|JMSException e) {
            LOG.error("Error loading context ", e);
        }

    }

    /**
     * create destination
     */
    private void createDestination(){
        try {
            queueIn = (Destination) ctx.lookup(JMS_QUEUE_IN);
            queueOut = (Destination) ctx.lookup(JMS_QUEUE_OUT);
            connection.start();
        } catch (NamingException|JMSException e) {
            LOG.error("Error create destination", e);
        }
    }

    /**
     * create producer and consumer
     */
    private void createProducerAndConsumer(){
        try {
            producer = session.createProducer(queueIn);
            consumer = session.createConsumer(queueOut);
        } catch (JMSException e) {
            LOG.error("Error creating produces/consumer",e);
        }
    }


    /**
     * close resources
     */
    private void closeResources(){
        try {
            producer.close();
            consumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
            LOG.error("Error closing resources", e);
        }
        //FIXME delete me
        System.exit(0);
    }

    /**
     * main
     * @param args params
     */
    public static void main(String[] args) {
       new JMSClient().execute();
    }



}
