package centralized.system.peer;

import centralized.system.peer.event.JoinPeer;
import centralized.system.peer.event.PublishPeer;
import centralized.system.peer.event.StartServer;
import centralized.system.peer.event.SubscribePeer;
import centralized.system.peer.event.SubscriptionInit;
import centralized.system.peer.event.UnsubscribePeer;
import se.sics.kompics.PortType;

public class PeerPort extends PortType {{
	negative(JoinPeer.class);
	negative(StartServer.class);
	negative(SubscribePeer.class);
	negative(UnsubscribePeer.class);
	negative(PublishPeer.class);
	negative(SubscriptionInit.class);
}}
