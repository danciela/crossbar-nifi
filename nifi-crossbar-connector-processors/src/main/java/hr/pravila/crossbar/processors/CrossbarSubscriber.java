/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hr.pravila.crossbar.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import ws.wamp.jawampa.WampClient;
import ws.wamp.jawampa.WampClientBuilder;
import ws.wamp.jawampa.connection.IWampConnectorProvider;
import ws.wamp.jawampa.transport.netty.NettyWampClientConnectorProvider;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class CrossbarSubscriber extends AbstractProcessor {

    public static final PropertyDescriptor CROSSBAR_URL = new PropertyDescriptor.Builder().name("URL")
            .displayName("URL").description("Crossbar url address.").required(true)
            .defaultValue("ws://127.0.0.1:8080/ws").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor CROSSBAR_REALM = new PropertyDescriptor.Builder().name("Realm")
            .displayName("Realm").description("Crossbar realm.").required(true).defaultValue("realm1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor SUB_TOPIC = new PropertyDescriptor.Builder().name("Topic")
            .displayName("Topic").description("Name od topic for subscription").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("succes")
            .description("Success relationship").build();

    public static final Relationship FAILED = new Relationship.Builder().name("succes")
            .description("Failed relationship").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    WampClient client;

    // Subscription addProcSubscription;
    // Subscription counterPublication;
    Subscription onHelloSubscription;

    // Scheduler for this example
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Scheduler rxScheduler = Schedulers.from(executor);

    static final int TIMER_INTERVAL = 1000; // 1s
    int counter = 0;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CROSSBAR_URL);
        descriptors.add(CROSSBAR_REALM);
        descriptors.add(SUB_TOPIC);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        ComponentLog logger = getLogger();
        logger.info("Crossbar subscriber processor started.");

        WampClientBuilder builder = new WampClientBuilder();
        IWampConnectorProvider connectorProvider = new NettyWampClientConnectorProvider();
        try {

            builder.withConnectorProvider(connectorProvider).withUri(context.getProperty(CROSSBAR_URL).getValue())
                    .withRealm(context.getProperty(CROSSBAR_REALM).getValue()).withInfiniteReconnects()
                    .withCloseOnErrors(true).withReconnectInterval(5, TimeUnit.SECONDS);
            client = builder.build();
            logger.info("Crossbar subscriber processor build.");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // Subscribe on the clients status updates
        client.statusChanged().observeOn(rxScheduler).subscribe(new Action1<WampClient.State>() {
            @Override
            public void call(WampClient.State t1) {
                System.out.println("Session status changed to " + t1);

                if (t1 instanceof WampClient.ConnectedState) {

                    // SUBSCRIBE to a topic and receive events
                    onHelloSubscription = client
                            .makeSubscription(context.getProperty(SUB_TOPIC).getValue(), String.class)
                            .observeOn(rxScheduler).subscribe(new Action1<String>() {
                                @Override
                                public void call(String msg) {
                                    logger.info ("Call method non subscription. Topic:" + context.getProperty(SUB_TOPIC).getValue());
                                    Map<PropertyDescriptor, String> processorProperties = context.getProperties();
                                    Map<String, String> generatedAttributes = new HashMap<String, String>();
                                    for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties
                                            .entrySet()) {
                                        PropertyDescriptor property = entry.getKey();
                                        if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                                            String dynamicValue = context.getProperty(property)
                                                    .evaluateAttributeExpressions().getValue();
                                            generatedAttributes.put(property.getName(), dynamicValue);
                                        }
                                    }

                                    FlowFile flowFile = session.create();

                                    flowFile = session.putAllAttributes(flowFile, generatedAttributes);
                                    flowFile = session.putAttribute(flowFile, "Message", msg);

                                    session.getProvenanceReporter().create(flowFile);
                                    session.transfer(flowFile, SUCCESS);

                                    logger.info("event for received: " + msg);
                                }
                            }, new Action1<Throwable>() {
                                @Override
                                public void call(Throwable e) {
                                    logger.info("Failed to subscribe: " + e);
                                }
                            }, new Action0() {
                                @Override
                                public void call() {
                                    logger.info("Subscription ended");
                                }
                            });

                } else if (t1 instanceof WampClient.DisconnectedState) {
                    closeSubscriptions();
                }
            }
        }, new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {
                System.out.println("Session ended with error " + t);
            }
        }, new Action0() {
            @Override
            public void call() {
                System.out.println("Session ended normally");
            }
        });

        client.open();
        // waitUntilKeypressed();
        //logger.info("Crossbar connector: Shutting down");
    }

    @OnStopped
    public void onStopped(ProcessContext context) {
        if (client != null) {
            closeSubscriptions();
            client.close();
            try {
                client.getTerminationFuture().get();
            } catch (Exception e) {
                // logger.error("Error: " + e.getStackTrace().toString());
            }
            executor.shutdown();
        }
    }

    /**
     * Close all subscriptions (registered events + procedures) and shut down all
     * timers (doing event publication and calls)
     */
    void closeSubscriptions() {
        if (onHelloSubscription != null)
            onHelloSubscription.unsubscribe();
        onHelloSubscription = null;
    }
}
