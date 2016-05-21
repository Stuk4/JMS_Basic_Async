package edu.cibertec.jaad.jms;

import org.apache.log4j.Logger;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Created by Java-VS on 21/05/2016.
 */
public class JMSServer implements MessageListener{


    public static final Logger LOG = Logger.getLogger(JMSServer.class);
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
        try {
            createConnectionAndSession();
            createDestination();
            // han sido creo la conexion y session y destinos
            consumer = session.createConsumer(queueIn);
            consumer.setMessageListener(this);
            LOG.info("esperando por mensaje ....");
        } catch (JMSException e) {
            e.printStackTrace();
        }


    }


    @Override
    public void onMessage(Message message) {
        try {
            // recepcion del msg
            MapMessage msg = (MapMessage) message;
            LOG.info("REcibido " + msg);
            LOG.info("operacion " + msg.getString("OPERACION"));
            LOG.info("MONTO " + msg.getDouble("MONTO"));


            // responder

            MessageProducer producer = session.createProducer(queueOut);
            TextMessage msgResp = session.createTextMessage("OK");
            producer.send(msgResp);
            producer.close();
            LOG.info("Mensaje Enviado " + msgResp   );

        } catch (Exception e) {
            e.printStackTrace();
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
        } catch (NamingException |JMSException e) {
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
            producer = session.createProducer(queueOut); // es al revez del cliente por que es el
            consumer = session.createConsumer(queueIn);  // servidor el que esta consumiendo
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
     * @param args
     */
    public static void main(String[] args) {
        JMSServer server = new JMSServer();
        server.execute();
    }



}
