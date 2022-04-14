
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.onlab.packet.DeserializationException;
import org.onlab.packet.Ethernet;
import org.onosproject.grpc.models.EventNotificationGrpc;
import org.onosproject.grpc.models.EventNotificationGrpc.EventNotificationStub;
import org.onosproject.grpc.models.EventNotificationProto;
import org.onosproject.grpc.models.EventNotificationProto.Notification;
import org.onosproject.grpc.models.EventNotificationProto.RegistrationRequest;
import org.onosproject.grpc.models.EventNotificationProto.RegistrationResponse;
import org.onosproject.grpc.models.EventNotificationProto.topicType;
import org.onosproject.grpc.models.PacketContextProtoOuterClass.PacketContextProto;
import java.util.logging.Logger;

public class EventMatchingManager {

    private static Logger log = Logger.getLogger(EventMatchingManager.class.getName());

    static String serverId = null;
    static String clientId = "EventMatching";

    public static void main(String[] args) {

        ManagedChannel channel;
        EventNotificationStub clientStub;

        // Create a managed gRPC channel
        channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", 50051)
                .usePlaintext()
                .build();

        // Assigns packetNotification stub to the gRPC channel.
        clientStub = EventNotificationGrpc.newStub(channel);

        //Building a request for clientId
        RegistrationRequest request = RegistrationRequest
                .newBuilder()
                .setClientId(clientId)
                .build();

        //Register the client app
        clientStub.register(
                request, new StreamObserver<>() {
                    @Override
                    public void onNext(RegistrationResponse value) {
                        serverId = value.getServerId();
                    }
                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onCompleted() {
                        log.info("client registered");
                    }
                }
        );

        log.info("server asjdashdkhj id is "+ serverId);

        //Creates a packet event topic
        EventNotificationProto.Topic packetTopic =
                EventNotificationProto.Topic.newBuilder()
                        .setClientId(clientId)
                        .setType(topicType.PACKET_EVENT)
                        .build();

       //  Implements a packet matching processor
        class PacketEvent implements Runnable {
            @Override
            public void run() {
                clientStub.onEvent(packetTopic, new StreamObserver<>() {
                            @Override
                            public void onNext(Notification value) {
                                PacketContextProto packetContextProto = value.getPacketContext();
                                PacketContextProto finalPacketContextProto = packetContextProto;

                                byte[] packetByteArray =
                                        finalPacketContextProto.getInboundPacket().getData().toByteArray();

                                Ethernet eth = new Ethernet();

                                try {
                                    eth = Ethernet.deserializer()
                                            .deserialize(packetByteArray, 0, packetByteArray.length);
                                } catch (DeserializationException e) {
                                    e.printStackTrace();
                                }
                                if (eth == null) {
                                    log.info("The received Packet is null");
                                    return;
                                } else {
                                    long type = eth.getEtherType();

                                    // Handle ARP messages
                                    if (type == Ethernet.TYPE_ARP) {
                                        log.info("An ARP Packet has been received");
                                    }
                                    // Handle Ethernet messages
                                    else if (type == Ethernet.TYPE_IPV4) {
                                        log.info("An IPV4 Packet has been received");
                                    } else {
                                        log.info("other packets");
                                    }
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                log.info("Packet Notification Completed");
                            }
                        }
                  );

                while (true) {}
            }
        }
        // Creates an instance of internal packet event class.
        PacketEvent packetEvent = new PacketEvent();
        Thread t = new Thread(packetEvent);
        t.start();
    }
}
