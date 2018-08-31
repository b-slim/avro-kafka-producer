package com.slim.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class AvroProducer {

  private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

  private final Random random = new Random();
  private final KafkaProducer<byte[], byte[]> producer;
  private final List<GenericRecord> records;
  private final Schema schema;
  private static final SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

  public AvroProducer(KafkaProducer<byte[], byte[]> producer, List<GenericRecord> records, Schema schema) {
    this.producer = producer;
    this.records = records;
    this.schema = schema;
  }

  public void produce(String topic) {
    final DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
    final int listSize = records.size();
    records.stream().forEach(genericRecord -> {

      if (random.nextDouble() < 10.0 / listSize) {
        System.out.println("Lucky Record" + genericRecord.toString());
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      try {
        writer.write(genericRecord, encoder);
        encoder.flush();
        out.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      byte[] serializedBytes = out.toByteArray();
      ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, serializedBytes);
      producer.send(producerRecord);
    });
  }

  static KafkaProducer<byte[], byte[]> getProducer(String serverString) {
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", serverString == null ? "127.0.0.1:9092" : serverString);
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("max.block.ms", "10000");
    return new KafkaProducer<>(producerProps);
  }

  public static void main(String[] args) {
    final int numRecords = Integer.valueOf(System.getProperty("kafka.num.records", "500"));
    List<GenericRecord> records = new ArrayList<>();
    for (int l = 0; l < numRecords; l++) {
      records.add(Wikipedia.newBuilder()
          .setTimestamp(formatter.format(new Timestamp(System.currentTimeMillis() + 1000 * l)))
          .setAdded(l * 300)
          .setDeleted(-l)
          .setIsrobot(l % 2 == 0)
          .setChannel("chanel-" + UUID.randomUUID())
          .setComment("comment-" + UUID.randomUUID())
          .setCommentlength(l)
          .setDiffurl(String.format("url %s", l))
          .setFlags("flag-" + UUID.randomUUID())
          .setIsminor(l % 2 > 0)
          .setIsanonymous(l % 2 == 0)
          .setNamespace("namespace")
          .setIsunpatrolled(new Boolean(true))
          .setIsnew(new Boolean(l % 2 > 0))
          .setPage(String.format("page %s", l * 100))
          .setDelta(l)
          .setDeltabucket(l * 100.4)
          .setUser("test-user-" + l)
          .build());
    }

    String connectionString = System.getProperty("bootstrap.servers", null);
    KafkaProducer producer = getProducer(connectionString);
    AvroProducer avroProducer = new AvroProducer(producer, records, Wikipedia.getClassSchema());
    String topic = System.getProperty("kafka.topic", "testing-topic");
    log.info("Sending [{}] records to Topic [{}] with Connection string [{}]", numRecords, topic, connectionString);
    avroProducer.produce(topic);
    producer.close();
  }

}
