package ru.stepan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Main {

    private final static String QUEUE = "ControlWork";

    public static void main(String[] args) throws Exception {
        ObjectMapper om = new ObjectMapper();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("stepan.hdfs.master.mooo.com");
        factory.setUsername("stepan");
        factory.setPassword("stepan");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        while (true) {
//        for (int i = 0; i < 100000; i++) {
            Data data = new Data();
            channel.basicPublish("", QUEUE, null, om.writeValueAsBytes(data));
//            System.out.println("Published " + om.writeValueAsString(data));
        }
//        channel.close();
//        connection.close();
    }
}
