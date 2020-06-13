package cs523.SparkHbase;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


import scala.Tuple2;

public class SparkKafkaConsumer {
	private static final String TABLE_NAME = "pandemic";
	private static final String CF_DATA = "data";
    private static Integer count =0;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream")
				.setMaster("local[*]");
    //    sparkConf.set("spark.streaming.backpressure.enabled","true");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(
				60000));
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("zookeeper.sync.time.ms", "60000");
        kafkaParams.put("auto.commit.interval.ms", "60000");



        kafkaParams.put("group.id", "BASICS");
		Set<String> topicName = Collections.singleton("mytesttopic");
		JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicName);
		JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream
				.map(new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 1L;
					public String call(Tuple2<String, String> tuple2) {
					    System.out.println("Input from Kafka "+tuple2._2);
                        return  hbaseRun(tuple2._2);
					}
				});
		kafkaSparkInputDStream.print();
        ssc.start();
		ssc.awaitTermination();
		System.out.println("Count of rows inserted: "+count);
	}

	private static String hbaseRun(String line) {
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
			 Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			@SuppressWarnings("deprecation")
			HTable tableDML = new HTable(config, TABLE_NAME);
			table.addFamily(new HColumnDescriptor(CF_DATA).setCompressionType(Algorithm.NONE));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
                System.out.print("Modifying table.... ");
                modifyTable(tableDML,line);

                //admin.disableTable(table.getTableName());
				//admin.deleteTable(table.getTableName());
			}
		else{
		    admin.createTable(table);
            }
			System.out.print("Modifying table.... ");
		//	modifyTable(tableDML);
			//uncomment this part to see the update of Bob's promotion
			//	tableDML.put(updateTable());
			System.out.println(" Done!");
		}catch(IOException e){

        }

        System.out.print("Modifying table....  "+count);

        return "created";
	}

	@SuppressWarnings("deprecation")
	private static void modifyTable(HTable tableDML, String line) throws IOException {
		List<Put> putData = new ArrayList<>();
	//	List<String> data = new ArrayList<>();
//		data.add("row7|7|John|Boston|Manager|150,000");
//		data.add("row8|8|Mary|New York|Sr. Engineer|130,000");
//		data.add("row9|9|Bob|Fremont|Jr. Engineer|90,000");
	//	for (String listValues : line.split("\\|");) {
        if(line!=null&&!line.isEmpty()) {
            String[] val = line.split(",");
            if(val.length==11 && !val[0].equals("step")) {
                Put p = new Put(Bytes.toBytes("row"+count));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("Step"), Bytes.toBytes(val[0]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("Type"), Bytes.toBytes(val[1]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("Amount"), Bytes.toBytes(val[2]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("Name_Orig"), Bytes.toBytes(val[3]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("Oldbalance_Org"), Bytes.toBytes(val[4]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("NewbalanceOrig"), Bytes.toBytes(val[5]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("nameDest"), Bytes.toBytes(val[6]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("oldbalanceDest"), Bytes.toBytes(val[7]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("newbalanceDest"), Bytes.toBytes(val[8]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("isFraud"), Bytes.toBytes(val[9]));
                p.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("isFlaggedFraud"), Bytes.toBytes(val[10]));

                putData.add(p);
                count++;
            }
            //	}
            tableDML.put(putData);
        }
	}


    //@SuppressWarnings("deprecation")
//	private static List<Put> updateTable() throws IOException, ParseException {
//		List<Put> putData = new ArrayList<>();
//		List<String> data = new ArrayList<>();
//		data.add("row3|3|Bob|Fremont|Sr. Engineer|90,000");
//		for (String listValues : data) {
//			String[] val = listValues.split("\\|");
//			Put p = new Put(Bytes.toBytes(val[0]));
//			p.add(Bytes.toBytes(CF_ROW_KEY),Bytes.toBytes("Emp ID"),Bytes.toBytes(val[1]));
//			p.add(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("Name"),Bytes.toBytes(val[2]));
//			p.add(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("City"),Bytes.toBytes(val[3]));
//			p.add(Bytes.toBytes(CF_PROFESSIONAL_DATA),Bytes.toBytes("Designation"),Bytes.toBytes(val[4]));
//			NumberFormat nf = NumberFormat.getInstance();
//			Double salaryUpdate = nf.parse(val[5]).doubleValue()+nf.parse(val[5]).doubleValue() *0.05;
//			p.add(Bytes.toBytes(CF_PROFESSIONAL_DATA),Bytes.toBytes("Salary"),Bytes.toBytes(salaryUpdate.toString()));
//			putData.add(p);
//		}
//		return putData;
//	}
}
