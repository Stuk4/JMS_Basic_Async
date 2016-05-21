package edu.cibertec.jaad.jms;

import org.apache.log4j.Logger;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

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
        processMessage();
        closeResources();
    }

    /**
     * process Message
     */
    public void processMessage(){

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
        try {


        } catch (Exception e) {
            LOG.error("Error procesando el mensaje ",e);
        }
    }



}
