/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.Map;
import java.util.Properties;


/**
 * Bolt implementation that can send Tuple data to Kafka.
 * <p/>
 * Most configuration for this bolt should be through the various 
 * setter methods in the bolt.
 * For backwards compatibility it supports the producer
 * configuration and topic to be placed in the storm config under
 * <p/>
 * 'kafka.broker.properties' and 'topic'
 * <p/>
 * respectively.
 */
public class MyKafkaBolt<K, V> extends BaseTickTupleAwareRichBolt {
    private static final long serialVersionUID = -5205886631877033478L;

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaBolt.class);

    public static final String TOPIC = "topic";

    private KafkaProducer<K, V> producer;
    private OutputCollector collector;
    private TupleToKafkaMapper<K,V> mapper;
    private KafkaTopicSelector topicSelector;
    private Properties boltSpecifiedProperties = new Properties();

    // Variable for Performance Evaluation 
    private static long rcv_number_of_tuples;
    private long lead_time;
    private long tuple_size;
    private long total_tuple_size;
    private long avg_total_lead_time;
    
    /**
     * {@see KafkaBolt#setFireAndForget(boolean)} for more details on this. 
     */
    private boolean fireAndForget = false;
    /**
     * {@see KafkaBolt#setAsync(boolean)} for more details on this. 
     */
    private boolean async = true;

    public MyKafkaBolt() {}

    public MyKafkaBolt<K,V> withTupleToKafkaMapper(TupleToKafkaMapper<K,V> mapper) {
        this.mapper = mapper;
        return this;
    }

    /**
     * Set the messages to be published to a single topic
     * @param topic the topic to publish to
     * @return this
     */
    public MyKafkaBolt<K, V> withTopicSelector(String topic) {
        return withTopicSelector(new DefaultTopicSelector(topic));
    }
    
    public MyKafkaBolt<K,V> withTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    public MyKafkaBolt<K,V> withProducerProperties(Properties producerProperties) {
        this.boltSpecifiedProperties = producerProperties;
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
       
    	rcv_number_of_tuples = 0l;
    	lead_time = 0l;
    	tuple_size = 0l;
    	total_tuple_size = 0l;
    	avg_total_lead_time = 0l;
    	
    	LOG.info("Preparing bolt with configuration {}", this);
        //for backward compatibility.
        if (mapper == null) {
            LOG.info("Mapper not specified. Setting default mapper to {}", FieldNameBasedTupleToKafkaMapper.class.getSimpleName());
            this.mapper = new FieldNameBasedTupleToKafkaMapper<K,V>();
        }

        //for backward compatibility.
        if (topicSelector == null) {
            if (stormConf.containsKey(TOPIC)) {
                LOG.info("TopicSelector not specified. Using [{}] for topic [{}] specified in bolt configuration,",
                        DefaultTopicSelector.class.getSimpleName(), stormConf.get(TOPIC));
                this.topicSelector = new DefaultTopicSelector((String) stormConf.get(TOPIC));
            } else {
                throw new IllegalStateException("topic should be specified in bolt's configuration");
            }
        }

        producer = mkProducer(boltSpecifiedProperties);
        this.collector = collector;
    }
    
    /**
     * Intended to be overridden for tests.  Make the producer with the given props
     */
    protected KafkaProducer<K, V> mkProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    @Override
    protected void process(final Tuple input) {
    	//LOG.info("@@@@@@@@@ KAFKABOLT READY TO CHANGED!!",this);
    	//System.out.println("KAFKABOLT READY TO CHANGED");
        K key = null;
        V message = null;
        String topic = null;
        try {
            key = mapper.getKeyFromTuple(input);
            message = mapper.getMessageFromTuple(input);
            topic = topicSelector.getTopic(input);
            if (topic != null ) {
                Callback callback = null;

                if (!fireAndForget && async) {
                    callback = new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata ignored, Exception e) {
                            synchronized (collector) {
                                if (e != null) {
                                    collector.reportError(e);
                                    collector.fail(input);
                                } else {
                                    collector.ack(input);
                                }
                            }
                        }
                    };
                }
                Future<RecordMetadata> result = producer.send(new ProducerRecord<K, V>(topic, key, message), callback);
                if (!async) {
                    try {
                        result.get();
                        collector.ack(input);
                    } catch (ExecutionException err) {
                        collector.reportError(err);
                        collector.fail(input);
                    }
                } else if (fireAndForget) {
                    collector.ack(input);
                }
            } else {
                LOG.warn("skipping key = " + key + ", topic selector returned null.");
                collector.ack(input);
            }
        } catch (Exception ex) {
            collector.reportError(ex);
            collector.fail(input);
        } finally {
        	lead_time = System.currentTimeMillis() - input.getLongByField("start-time");
        	//lead_time += System.currentTimeMillis() - input.getLongByField("start-time");
        	if(lead_time > 0l) {
        		avg_total_lead_time += lead_time;        	
        		
        	}
        	rcv_number_of_tuples++;
        	//tuple_size += (input.getBinary(0)).length;
        	total_tuple_size += input.getLongByField("total_size");
        	tuple_size += input.getLongByField("size");
        	//LOG.info("###latency: " + lead_time );
        	//LOG.info("\n$$$$ Bolt cleanup, total_rcv_number_of_tuples: " + rcv_number_of_tuples + " /// avg_latency_time,"+((double)avg_total_lead_time/(double)rcv_number_of_tuples) + " /// Kafkabolt TUPLE SIZE: " +  tuple_size + " ///Total Tuple Size: " + total_tuple_size);
        	//LOG.info("\n$$$$ Bolt cleanup,total_rcv_number_of_tuples," + rcv_number_of_tuples+",avg_time,"+((double)lead_time/(double)rcv_number_of_tuples));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
    	LOG.info("\n$$$$ Bolt cleanup, total_rcv_number_of_tuples: " + rcv_number_of_tuples + " /// avg_latency_time,"+((double)avg_total_lead_time/(double)rcv_number_of_tuples) + " /// Kafkabolt TUPLE SIZE: " +  tuple_size + " ///Total Tuple Size(Now num): " + total_tuple_size);
        producer.close();
    }

    /**
     * If set to true the bolt will assume that sending a message to kafka will succeed and will ack
     * the tuple as soon as it has handed the message off to the producer API
     * if false (the default) the message will be acked after it was successfully sent to kafka or
     * failed if it was not successfully sent.
     * @param fireAndForget
     */
    public void setFireAndForget(boolean fireAndForget) {
        this.fireAndForget = fireAndForget;
    }

    /**
     * If set to true(the default) the bolt will not wait for the message
     * to be fully sent to Kafka before getting another tuple to send.
     * @param async true to have multiple tuples in flight to kafka, else false.
     */
    public void setAsync(boolean async) {
        this.async = async;
    }
    
    @Override
    public String toString() {
        return "KafkaBolt: {mapper: " + mapper +
                " topicSelector: " + topicSelector +
                " fireAndForget: " + fireAndForget +
                " async: " + async +
                " proerties: " + boltSpecifiedProperties;
    }
}