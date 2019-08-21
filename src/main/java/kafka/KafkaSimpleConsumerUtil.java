package kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaSimpleConsumerUtil {
    private static final Logger LOGGER = Logger.getLogger(KafkaSimpleConsumerUtil.class);
	private static List<String> M_REPLICABROKERS = new ArrayList<String>();

	/**
	 * 
	 * getTopicPartitionOffset:分区数量 <br/>
	 * @param a_topic  topic
	 * @param a_partition   分区
	 * @param a_seedBrokers  主节点
	 * @param a_port   端口
	 * @param logger   日志
	 * @return  结果集
	 * @throws Exception
	 *             执行异常  <br/>
	 */
	public static long getTopicPartitionOffset(String a_topic, int a_partition,
			List<String> a_seedBrokers, int a_port)
			throws Exception {
		long currentOffset = -1;
		// 获取指定Topic partition的元数据
		PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic,
				a_partition);
		if (metadata == null) {
		    LOGGER.error("Can't find metadata for Topic and Partition. Exiting：topic"
                    + ":" + a_topic + ":parition:" + a_partition);
			return currentOffset;
		}
		if (metadata.leader() == null) {
		    LOGGER.error("Can't find Leader for Topic and Partition. Exiting：topic"
                    + ":" + a_topic + ":parition:" + a_partition);
			return currentOffset;
		}
		//获取分区的主节点
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + a_topic + "_" + a_partition ;
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port,
				100000, 64 * 1024, clientName);
		//获取分区里边的数据量
		currentOffset = getLastOffset(consumer, a_topic, a_partition,
				kafka.api.OffsetRequest.LatestTime(), clientName);
		if (consumer != null) {
			consumer.close();
		}
		return currentOffset;
	}
	
	public static long getTopicPartitionOffset1()
			throws Exception {
		SimpleConsumer consumer = new SimpleConsumer("10.221.5.142", 9093,
				100000, 64 * 1024, "Client_z_test_20190430_0");
		TopicAndPartition topicAndPartition = new TopicAndPartition("z_test_20190430",
				0);
		TopicAndPartition topicAndPartition1 = new TopicAndPartition("z_test_20190430",
				1);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.LatestTime(), 1));
		/*requestInfo.put(topicAndPartition1, new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.LatestTime(), 1));*/
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				"Client_z_test_20190430_0");
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
		    LOGGER.error("Error fetching data Offset Data the Broker. Reason: "
                    + response.errorCode("z_test_20190430", 0));
			return 0;
		}
		long[] offsets = response.offsets("z_test_20190430", 0);
		//long[] offsets1 = response.offsets("z_test_20190430", 1);
		return offsets[0];
	}
	/**
	 * 
	 * getLastOffset:主题分区的数据量<br/>
	 * @param consumer  参数
	 * @param topic     主题
	 * @param partition  分区
	 * @param whichTime  时间
	 * @param clientName  客户名字
	 * @return
	 *            结果集 <br/>
	 */
	public static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
		    LOGGER.error("Error fetching data Offset Data the Broker. Reason: "
                    + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	/**
	 * 
	 * findLeader:获取元数据 <br/>
	 * @param a_seedBrokers   节 点
	 * @param a_port          端口
	 * @param a_topic         主题
	 * @param a_partition     分区
	 * @return
	 *                        结果集 <br/>
	 */
	private static PartitionMetadata findLeader(List<String> a_seedBrokers,
			int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
						"leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
			    LOGGER.error("Error communicating with Broker [" + seed
                        + "] to find Leader for [" + a_topic + ", "
                        + a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			M_REPLICABROKERS.clear();
			for (BrokerEndPoint replica : returnMetaData.replicas()) {
				M_REPLICABROKERS.add(replica.host());
			}
		}
		return returnMetaData;
	}

	public static void main(String args[]) {
		KafkaSimpleConsumerUtil example = new KafkaSimpleConsumerUtil();
		// 要订阅的topic
		String topic = "wmqq";
		// 要查找的分区
		int partition = Integer.parseInt("4");
		// broker节点的ip
		List<String> seeds = new ArrayList<String>();
//		seeds.add("172.30.18.91");
//		seeds.add("172.30.18.92");
//		seeds.add("172.30.18.93");
		seeds.add("172.30.18.94");
		seeds.add("172.30.18.95");
		// 端口
		int port = Integer.parseInt("9093");
        		try {
			long ss = example.getTopicPartitionOffset(topic, partition, seeds, port);
			System.out.println(ss);
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	
	}

}
