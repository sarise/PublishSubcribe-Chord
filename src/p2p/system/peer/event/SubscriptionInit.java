package p2p.system.peer.event;

import java.math.BigInteger;
import java.util.Vector;

import p2p.system.peer.PeerAddress;


import se.sics.kompics.Event;

public class SubscriptionInit extends Event {
	
	private final Vector<BigInteger> topicIDs;

//-------------------------------------------------------------------
	public SubscriptionInit(Vector<BigInteger> topicIDs) {
		this.topicIDs = topicIDs;
	}
	
	public Vector<BigInteger> getTopicIDs() {
		return this.topicIDs;
	}

}
