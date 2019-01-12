package com.hive.kafka;

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
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class AvroProducer {

  private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

  private static final Random random = new Random();
  private final KafkaProducer<byte[], byte[]> producer;
  private final List<GenericRecord> records;
  private final Schema schema;
  private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
      ProducerRecord<byte[], byte[]>
          producerRecord =
          new ProducerRecord<>(topic, UUID.randomUUID().toString().getBytes(), serializedBytes);
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

    final int numRecords = Integer.valueOf(System.getProperty("kafka.num.records", "5000"));
    List<GenericRecord> records = new ArrayList<>();
    final String
        connectionString =
        System.getProperty("bootstrap.servers", "ctr-e138-1518143905142-564127-01-000006.hwx.site:6667");
    KafkaProducer producer = getProducer(connectionString);

    final AtomicBoolean started = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("logging off");
      started.set(false);
      producer.flush();
      producer.close();
      log.info("Shutdown Hook is running !");
    }));

    while (started.get()) {
      for (int l = 0; l < numRecords; l++) {
        int num_states = State.values().length;
        records.add(Order.newBuilder()
            .setTime$1(formatter.format(new Timestamp(System.currentTimeMillis() + 1000 * l)))
            .setSsProductId(random.nextInt(3_000))
            .setSsCustomerId(random.nextInt(800_000))
            .setSsOrderNumber(random.nextInt(1_000_000))
            .setSsOrderType("type_" + UUID.randomUUID())
            .setSsQuantity(random.nextInt(10_000))
            .setSsStoreState(State.values()[random.nextInt(num_states)].state)
            .setSsStoreCounty(Locale.getISOCountries()[random.nextInt(Locale.getISOCountries().length)])
            .setSsSalesPrice(random.nextInt(1_000_000))
            .setSsStoreCity("City-" + random.nextInt(1000))
            .build());

      }

      AvroProducer avroProducer = new AvroProducer(producer, records, Order.getClassSchema());
      String topic = System.getProperty("kafka.topic", "testing-topic-3");
      log.info("Sending [{}] records to Topic [{}] with Connection string [{}]", numRecords, topic, connectionString);
      if (started.get()) {
        avroProducer.produce(topic);
      } else {
        break;
      }
      try {
        log.info("Sleeping");
        Thread.sleep(1000);
        log.info("Sleeping is done");
      } catch (InterruptedException e) {
        log.error("InterruptedException", e);
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unused") public enum State {
    AL("Alabama"), MT("Montana"), AK("Alaska"), NE("Nebraska"), AZ("Arizona"), NV("Nevada"), AR("Arkansas"), NH(
        "NewHampshire"), CA("California"), NJ("NewJersey"), CO("Colorado"), NM("NewMexico"), CT("Connecticut"), NY(
        "NewYork"), DE("Delaware"), NC("NorthCarolina"), FL("Florida"), ND("NorthDakota"), GA("Georgia"), OH("Ohio"),
    HI(
        "Hawaii"), OK("Oklahoma"), ID("Idaho"), OR("Oregon"), IL("Illinois"), PA("Pennsylvania"), IN("Indiana"), RI(
        "RhodeIsland"), IA("Iowa"), SC("SouthCarolina"), KS("Kansas"), SD("SouthDakota"), KY("Kentucky"), TN(
            "Tennessee"), LA(
        "Louisiana"), TX("Texas"), ME("Maine"), UT("Utah"), MD("Maryland"), VT("Vermont"), MA("Massachusetts"), VA(
        "Virginia"), MI("Michigan"), WA("Washington"), MN("Minnesota"), WV("WestVirginia"), MS("Mississippi"), WI(
        "Wisconsin"), MO("Missouri"), WY("Wyoming");
    private String state;

    State(String state) {
      this.state = state;
    }

    public String getStatusCode() {
      return this.state;
    }
  }
}
