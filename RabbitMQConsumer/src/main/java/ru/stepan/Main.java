package ru.stepan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import control.work.Data;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Main {
    private final static String QUEUE = "ControlWork";
    //    private final static String HDFS_URL = "hdfs://stepan.hdfs.master.mooo.com:9000";
    private final static String HDFS_URL = "hdfs://master:9000";
    private final static int MAX_ROWS_IN_FILE = 100000;

    public static void main(String[] args) {
        BasicConfigurator.configure();
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(HDFS_URL), conf);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        final Path[] f = {new Path(URI.create(HDFS_URL + "/controlWork/" + UUID.randomUUID().toString() + ".avro"))};
        final OutputStream[] os;
        try {
            os = new OutputStream[]{fs.create(f[0])};
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        ObjectMapper objectMapper = new ObjectMapper();
//        Data data1 = objectMapper.readValue("{\"num\":676498,\"code\":4185,\"someString\":\"olrwp\"}", Data.class);

        DatumWriter<Data> dataDatumWriter = new SpecificDatumWriter<>(Data.class);
        DataFileWriter<Data> dataDataFileWriter = new DataFileWriter<>(dataDatumWriter);
        try {
            dataDataFileWriter.create(Data.getClassSchema(), os[0]);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("master");
        factory.setUsername("stepan");
        factory.setPassword("stepan");
        factory.setAutomaticRecoveryEnabled(true);
        Connection connection = null;
        try {
            connection = factory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException(e);
        }
        Channel channel = null;
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int channelNum = channel.getChannelNumber();

        final AtomicInteger[] border = {new AtomicInteger()};

        FileSystem finalFs = fs;
        FileSystem finalFs1 = fs;
        Connection finalConnection = connection;
        Channel finalChannel = channel;
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                if(border[0].get() >= MAX_ROWS_IN_FILE) {
                    os[0].close();
                    f[0] = new Path(URI.create(HDFS_URL + "/controlWork/" + UUID.randomUUID().toString() + ".avro"));
                    os[0] = finalFs.create(f[0]);
                    border[0].set(0);
                }

                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                Data data = objectMapper.readValue(message, Data.class);
                dataDataFileWriter.append(data);
                dataDataFileWriter.flush();
                border[0].getAndIncrement();
            } catch (ClosedChannelException e) {
                finalConnection.close();
                finalFs1.close();
                dataDataFileWriter.close();
                try {
                    finalChannel.close();
                } catch (TimeoutException ex) {
                    ex.printStackTrace();
                }
                throw new IllegalStateException(e);
            }

        };

        try {
            String tag = channel.basicConsume(QUEUE, true, deliverCallback, consumerTag -> { });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
