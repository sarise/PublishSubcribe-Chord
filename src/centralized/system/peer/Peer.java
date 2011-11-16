package centralized.system.peer;


import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

//import centralized.simulator.snapshot.Snapshot;
import centralized.simulator.scenarios.Scenario1;
import centralized.system.peer.event.JoinPeer;
import centralized.system.peer.event.PublishPeer;
import centralized.system.peer.event.SubscribePeer;
import centralized.system.peer.event.SubscriptionInit;
import centralized.system.peer.event.UnsubscribePeer;
import centralized.system.peer.message.Notification;
import centralized.system.peer.message.Publication;
import centralized.system.peer.message.SubscribeRequest;
import centralized.system.peer.message.TopicList;
import centralized.system.peer.message.UnsubscribeRequest;


import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapCompleted;
import se.sics.kompics.p2p.bootstrap.BootstrapRequest;
import se.sics.kompics.p2p.bootstrap.BootstrapResponse;
import se.sics.kompics.p2p.bootstrap.P2pBootstrap;
import se.sics.kompics.p2p.bootstrap.PeerEntry;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClient;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClientInit;
import se.sics.kompics.p2p.fd.FailureDetector;
import se.sics.kompics.p2p.fd.PeerFailureSuspicion;
import se.sics.kompics.p2p.fd.StartProbingPeer;
import se.sics.kompics.p2p.fd.StopProbingPeer;
import se.sics.kompics.p2p.fd.SuspicionStatus;
import se.sics.kompics.p2p.fd.ping.PingFailureDetector;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorInit;
import se.sics.kompics.p2p.peer.FindSuccReply;
import se.sics.kompics.p2p.peer.Notify;
import se.sics.kompics.p2p.peer.PeriodicStabilization;
import se.sics.kompics.p2p.peer.RingKey;
import se.sics.kompics.p2p.peer.WhoIsPred;
import se.sics.kompics.p2p.peer.WhoIsPredReply;
import se.sics.kompics.p2p.simulator.launch.Configuration;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

import se.sics.kompics.p2p.peer.FindSucc;
import se.sics.kompics.p2p.simulator.snapshot.Snapshot;
import se.sics.kompics.p2p.simulator.snapshot.PeerInfo;

public final class Peer extends ComponentDefinition {
	
	public static BigInteger RING_SIZE = new BigInteger(2 + "").pow(Configuration.Log2Ring);
	public static int FINGER_SIZE = Configuration.Log2Ring;
	public static int SUCC_SIZE = Configuration.Log2Ring; // WOW! a peer has as much as the finger size??
	private static int WAIT_TIME_TO_REJOIN = 15;
	private static int STABILIZING_PERIOD = 1000;
	private PeerAddress pred;
	private PeerAddress succ;
	private PeerAddress[] fingers = new PeerAddress[FINGER_SIZE];
	private PeerAddress[] succList = new PeerAddress[SUCC_SIZE];
	
	private int fingerIndex = 0;
	private int joinCounter = 0;
	private boolean started = false;
	
	
	//======================
	
	Negative<PeerPort> msPeerPort = negative(PeerPort.class);

	Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);

	private Component fd, bootstrap;
	
	private Random rand;
	private Address myAddress;
	private Address serverAddress;
	private PeerAddress myPeerAddress;
	private PeerAddress serverPeerAddress;
	private Vector<PeerAddress> friends;
	private int msgPeriod;
	private int viewSize;
	private boolean bootstrapped;
	
	private HashMap<Address, UUID> fdRequests;
	private HashMap<Address, PeerAddress> fdPeers;
	
	private HashMap<BigInteger, BigInteger> mySubscriptions;				// <Topic ID, last sequence number>
	private HashMap<BigInteger, Vector<Notification>> eventRepository;		// <Topic ID, list of Notification>
	private HashMap<BigInteger, Set<Address>> forwardingTable;		// <Topic ID, list of PeerAddress (your
	
	private BigInteger publicationSeqNum;

//-------------------------------------------------------------------
	public Peer() {
		
//====================
		for (int i = 0; i < SUCC_SIZE; i++)
			this.succList[i] = null;

		for (int i = 0; i < FINGER_SIZE; i++)
			this.fingers[i] = null;

		
		//=========================
		fdRequests = new HashMap<Address, UUID>();
		fdPeers = new HashMap<Address, PeerAddress>();
		rand = new Random(System.currentTimeMillis());
		mySubscriptions =  new HashMap<BigInteger, BigInteger>();
		eventRepository = new HashMap<BigInteger, Vector<Notification>>();
		forwardingTable = new HashMap<BigInteger, Set<Address>>();

		fd = create(PingFailureDetector.class);
		bootstrap = create(BootstrapClient.class);
		
		publicationSeqNum = BigInteger.ONE;
		
		connect(network, fd.getNegative(Network.class));
		connect(network, bootstrap.getNegative(Network.class));
		connect(timer, fd.getNegative(Timer.class));
		connect(timer, bootstrap.getNegative(Timer.class));
		
		subscribe(handleInit, control);
		subscribe(handleStart, control);
		
//		subscribe(handleSendMessage, timer);
//		subscribe(handleRecvMessage, network);
		subscribe(handleJoin, msPeerPort);
		subscribe(handleSubscribe, msPeerPort);
		subscribe(handleUnsubscribe, msPeerPort);
		subscribe(handlePublish, msPeerPort);
		
		subscribe(handleSubscriptionInit, msPeerPort);

		
		subscribe(handleBootstrapResponse, bootstrap.getPositive(P2pBootstrap.class));
		subscribe(handlePeerFailureSuspicion, fd.getPositive(FailureDetector.class));
		
		
		subscribe(eventNotificationHandler, network);
		
		
		subscribe(subscribeHandler, network);
		subscribe(unsubscribeHandler, network);
		
		//=============
		subscribe(handlePeriodicStabilization, timer);
		subscribe(handleFindSucc, network);
		subscribe(handleFindSuccReply, network);
		subscribe(handleWhoIsPred, network);
		subscribe(handleWhoIsPredReply, network);
		subscribe(handleNotify, network);
		//=================
		// subscribe(messageHandler, network);
	}
//-------------------------------------------------------------------
// This handler initiates the Peer component.	
//-------------------------------------------------------------------
	Handler<PeerInit> handleInit = new Handler<PeerInit>() {
		@Override
		public void handle(PeerInit init) {
			
			myPeerAddress = init.getMSPeerSelf();
			myAddress = myPeerAddress.getPeerAddress();
			serverPeerAddress = init.getServerPeerAddress();
			serverAddress = serverPeerAddress.getPeerAddress(); //TODO: remove server
			friends = new Vector<PeerAddress>();
			msgPeriod = init.getMSConfiguration().getSnapshotPeriod();

			viewSize = init.getMSConfiguration().getViewSize();

			trigger(new BootstrapClientInit(myAddress, init.getBootstrapConfiguration()), bootstrap.getControl());
			trigger(new PingFailureDetectorInit(myAddress, init.getFdConfiguration()), fd.getControl());
			
			System.out.println("Peer " + myAddress.getId() + " is initialized.");
		}
	};




//-------------------------------------------------------------------
// Whenever a new node joins the system, this handler is triggered
// by the simulator.
// In this method the node sends a request to the bootstrap server
// to get a pre-defined number of existing nodes.
// You can change the number of requested nodes through peerConfiguration
// defined in Configuration.java.
// Here, the node adds itself to the Snapshot.
//-------------------------------------------------------------------
	Handler<JoinPeer> handleJoin = new Handler<JoinPeer>() {
		@Override
		public void handle(JoinPeer event) {
			Snapshot.addPeer(myPeerAddress);
			BootstrapRequest request = new BootstrapRequest("Lab0", viewSize); //("chord",1)
			trigger(request, bootstrap.getPositive(P2pBootstrap.class));			
		}
	};

//-------------------------------------------------------------------
// Whenever a node receives a response from the bootstrap server
// this handler is triggered.
// In this handler, the nodes adds the received list to its friend
// list and registers them in the failure detector.
// In addition, it sets a periodic scheduler to call the
// SendMessage handler periodically.	
//-------------------------------------------------------------------
	
	Handler<BootstrapResponse> handleBootstrapResponse = new Handler<BootstrapResponse>() {
		@Override
		public void handle(BootstrapResponse event) {
			if (!bootstrapped) {
				bootstrapped = true;
				PeerAddress peer;
				Set<PeerEntry> somePeers = event.getPeers();

				/*for (PeerEntry peerEntry : somePeers) {
					peer = (PeerAddress)peerEntry.getOverlayAddress();
					friends.addElement(peer);
					fdRegister(peer);
				}
				
				trigger(new BootstrapCompleted("Lab0", myPeerAddress), bootstrap.getPositive(P2pBootstrap.class));
				Snapshot.addFriends(myPeerAddress, friends);
				
				SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(msgPeriod, msgPeriod);
				spt.setTimeoutEvent(new SendMessage(spt));
				trigger(spt, timer);
*/

				
				if (somePeers.size() == 0) {
					pred = null;
					succ = myPeerAddress;
					succList[0] = succ;
					Snapshot.setPred(myPeerAddress, pred);
					Snapshot.setSucc(myPeerAddress, succ);
					joinCounter = -1;
					trigger(new BootstrapCompleted("chord", myPeerAddress), bootstrap.getPositive(P2pBootstrap.class));
				} else {
					pred = null;
					PeerAddress existingPeer = (PeerAddress)somePeers.iterator().next().getOverlayAddress();
					trigger(new FindSucc(myPeerAddress, existingPeer, myPeerAddress, myPeerAddress.getPeerId(), 0), network);
					Snapshot.setPred(myPeerAddress, pred);
				}

				if (!started) {
					SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(STABILIZING_PERIOD, STABILIZING_PERIOD);
					spt.setTimeoutEvent(new PeriodicStabilization(spt));
					trigger(spt, timer);
					started = true;
				}
			}
		}
	};
		
//		System.out.println("Peer subscribed to initHandler, startHandler, and eventNotificationHandler.");

	

//--------------------chord
	
//-------------------------------------------------------------------
	Handler<FindSucc> handleFindSucc = new Handler<FindSucc>() {
		public void handle(FindSucc event) {
			BigInteger id = event.getID();
			PeerAddress initiator = event.getInitiator();
			int fingerIndex = event.getFingerIndex();
			
			if (succ != null && RingKey.belongsTo(id, myPeerAddress.getPeerId(), succ.getPeerId(), RingKey.IntervalBounds.OPEN_CLOSED, RING_SIZE))
				trigger(new FindSuccReply(myPeerAddress, initiator, succ, fingerIndex), network);
			else {
				PeerAddress nextPeer = closestPrecedingNode(id);
				trigger(new FindSucc(myPeerAddress, nextPeer, initiator, id, fingerIndex), network);
			}
		}
	};
	
//-------------------------------------------------------------------
	Handler<FindSuccReply> handleFindSuccReply = new Handler<FindSuccReply>() {
		public void handle(FindSuccReply event) {
			PeerAddress responsible = event.getResponsible();
			int fingerIndex = event.getFingerIndex();

			if (fingerIndex == 0) {
				succ = new PeerAddress(responsible);
				succList[0] = new PeerAddress(responsible);
				Snapshot.setSucc(myPeerAddress, succ);
				trigger(new BootstrapCompleted("chord", myPeerAddress), bootstrap.getPositive(P2pBootstrap.class));
				fdRegister(succ);
				joinCounter = -1;
			}
			
			fingers[fingerIndex] = new PeerAddress(responsible);
			Snapshot.setFingers(myPeerAddress, fingers);
		}
	};
	
//-------------------------------------------------------------------
	Handler<PeriodicStabilization> handlePeriodicStabilization = new Handler<PeriodicStabilization>() {
		public void handle(PeriodicStabilization event) {
			//System.out.println("*********************");
			if (succ == null && joinCounter != -1) { // means we haven't joined the ring yet
				if (joinCounter++ > Peer.WAIT_TIME_TO_REJOIN) { // waited enough, time to retransmit my request
					joinCounter = 0;
					bootstrapped = false;

					BootstrapRequest request = new BootstrapRequest("chord", 1);
					trigger(request, bootstrap.getPositive(P2pBootstrap.class));			
				}
			} 
			
			if (succ != null)
				trigger(new WhoIsPred(myPeerAddress, succ), network);

			// fix fingers
			if (succ == null)
				return;

			fingerIndex++;
			if (fingerIndex == FINGER_SIZE)
				fingerIndex = 1;
			
			BigInteger index = new BigInteger(2 + "").pow(fingerIndex);			
			BigInteger id = myPeerAddress.getPeerId().add(index).mod(RING_SIZE); 
						
			if (RingKey.belongsTo(id, myPeerAddress.getPeerId(), succ.getPeerId(), RingKey.IntervalBounds.OPEN_CLOSED, RING_SIZE))
				fingers[fingerIndex] = new PeerAddress(succ);
			else {
				PeerAddress nextPeer = closestPrecedingNode(id);
				trigger(new FindSucc(myPeerAddress, nextPeer, myPeerAddress, id, fingerIndex), network);
			}
			
			/*
			System.out.print("Fingers for peer " + myAddress.getId() + ": ");
			for (int i = 0; i < fingers.length; i++)
				if (fingers[i] != null)
					System.out.print(fingers[i].getPeerId() + " ");
			System.out.println();
			*/
		}
	};

//-------------------------------------------------------------------
	Handler<WhoIsPred> handleWhoIsPred = new Handler<WhoIsPred>() {
		public void handle(WhoIsPred event) {
			PeerAddress requester = event.getMSPeerSource();
			trigger(new WhoIsPredReply(myPeerAddress, requester, pred, succList), network);
		}
	};

//-------------------------------------------------------------------
	Handler<WhoIsPredReply> handleWhoIsPredReply = new Handler<WhoIsPredReply>() {
		public void handle(WhoIsPredReply event) {
			PeerAddress succPred = event.getPred();
			PeerAddress[] succSuccList = event.getSuccList();

			if (succ == null)
				return;

			if (succPred != null) {
				if (RingKey.belongsTo(succPred.getPeerId(), myPeerAddress.getPeerId(), succ.getPeerId(), RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE)) {
					succ = new PeerAddress(succPred);
					fingers[0] = succ;
					succList[0] = succ;
					Snapshot.setSucc(myPeerAddress, succ);
					Snapshot.setFingers(myPeerAddress, fingers);
					fdRegister(succ);
					joinCounter = -1;
				}
			}
			
			for (int i = 1; i < succSuccList.length; i++) {
				if (succSuccList[i - 1] != null)
					succList[i] = new PeerAddress(succSuccList[i - 1]);
			}
			
			Snapshot.setSuccList(myPeerAddress, succList);

			if (succ != null)
				trigger(new Notify(myPeerAddress, succ, myPeerAddress), network);
		}
	};

//-------------------------------------------------------------------
	Handler<Notify> handleNotify = new Handler<Notify>() {
		public void handle(Notify event) {
			PeerAddress newPred = event.getID();
			
			if (pred == null || RingKey.belongsTo(newPred.getPeerId(), pred.getPeerId(), myPeerAddress.getPeerId(), RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE)) {
				pred = new PeerAddress(newPred);
				fdRegister(pred);
				Snapshot.setPred(myPeerAddress, newPred);
			}
		}
	};

	
//-------------------------------------------------------------------	
// If a node has registered for another node, e.g. P, this handler
// is triggered if P fails.
//-------------------------------------------------------------------	
	Handler<PeerFailureSuspicion> handlePeerFailureSuspicion = new Handler<PeerFailureSuspicion>() {
		@Override
		public void handle(PeerFailureSuspicion event) {
			Address suspectedPeerAddress = event.getPeerAddress();
			
			if (event.getSuspicionStatus().equals(SuspicionStatus.SUSPECTED)) {
				if (!fdPeers.containsKey(suspectedPeerAddress) || !fdRequests.containsKey(suspectedPeerAddress))
					return;
				
				PeerAddress suspectedPeer = fdPeers.get(suspectedPeerAddress);
				fdUnregister(suspectedPeer);
if (suspectedPeer.equals(pred))
					pred = null;
				
				if (suspectedPeer.equals(succ)) {
					int i;
					for(i = 1; i < Peer.SUCC_SIZE; i++) {
						if (succList[i] != null && !succList[i].equals(myPeerAddress) && !succList[i].equals(suspectedPeer)) {
							succ = succList[i];
							fingers[0] = succ;
							fdRegister(succ);
							break;
						} else
							succ = null;
					}
					
					joinCounter = 0;
					
					Snapshot.setSucc(myPeerAddress, succ);
					Snapshot.setFingers(myPeerAddress, fingers);
					
					for (; i > 0; i--)
						succList = leftshift(succList);
				}

				for(int i = 1; i < Peer.SUCC_SIZE; i++) {
					if (succList[i] != null && succList[i].equals(suspectedPeer))
						succList[i] = null;
				}

				/*
				friends.removeElement(suspectedPeer);
				System.out.println(myPeerAddress + " detects failure of " + suspectedPeer);
			}*/
}
		}
	};
	
//-------------------------------------------------------------------
	private PeerAddress closestPrecedingNode(BigInteger id) {
		for (int i = FINGER_SIZE - 1; i >= 0; i--) {
			if (fingers[i] != null && RingKey.belongsTo(fingers[i].getPeerId(), myPeerAddress.getPeerId(), id, RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE))
				return fingers[i];
		}
		
		return myPeerAddress;
	}
	

//-------------------------------------------------------------------
	private PeerAddress[] leftshift(PeerAddress[] list) {
		PeerAddress[] newList = new PeerAddress[list.length];
		
		for(int i = 1; i < list.length; i++)
			newList[i - 1] = list[i];
		
		newList[list.length - 1] = null;
		
		return newList;
	}



	Handler<Notification> eventNotificationHandler = new Handler<Notification>() {
		@Override
		public void handle(Notification msg) {
			System.out.println("Peer " + myAddress.getId() 
					+ " received a notification about " + msg.getTopic());
		}
	};
	
	Handler<UnsubscribeRequest> unsubscribeHandler = new Handler<UnsubscribeRequest>() {
		public void handle(UnsubscribeRequest msg) {
			// 
			System.out.println("- Peer " + myPeerAddress.getPeerId() + " received an UnsubcribeRequest.");
			
			UnsubscribeRequest newMsg = new UnsubscribeRequest(msg.getTopic(), myAddress, null);
			
			Set<Address> subscriberlist = forwardingTable.get(newMsg.getTopic());
			if (subscriberlist == null) {
				System.out.println("No entry in the forwarding table.");
				routeMessage(newMsg, newMsg.getTopic());
			}
			else {
				subscriberlist.remove(msg.getSource());
				if (subscriberlist.isEmpty()) {
					System.out.println("No more subscribers.");
					forwardingTable.remove(newMsg.getTopic());
					routeMessage(newMsg, newMsg.getTopic());
				} else {
					forwardingTable.put(newMsg.getTopic(), subscriberlist);
					System.out.println("Not forwarding the UnsubscribeRequest. subscriberlist: " + subscriberlist.toString());
				}
			}
		}
	};
	
	
	Handler<SubscribeRequest> subscribeHandler = new Handler<SubscribeRequest>() {
		public void handle(SubscribeRequest msg) {
			// 
			System.out.println("+ Peer " + myPeerAddress.getPeerId() + " received a SubcribeRequest.");
			
			// TODO: lastSequenceNum, should I check with the lastSequenceNum with respect to the forwarding table.
			SubscribeRequest newMsg = new SubscribeRequest(msg.getTopic(), msg.getLastSequenceNum(), myAddress, null);
			
			Set<Address> tmp = forwardingTable.get(newMsg.getTopic());
			if (tmp == null) {
				tmp = new HashSet<Address>();
			}
			tmp.add(msg.getSource());
			forwardingTable.put(newMsg.getTopic(), tmp);
				
			SubscribeRequest msg2 = new SubscribeRequest(
					newMsg.getTopic(), newMsg.getLastSequenceNum(), newMsg.getSource(), null);
			routeMessage(msg2, newMsg.getTopic());
		}
	};
	
	// Helper methods
	
	private boolean between(BigInteger destID, BigInteger predID, BigInteger myID) {
		if (destID.equals(myID))
			return true;
		
		if (predID.compareTo(myID) == 1)
			myID = myID.add(RING_SIZE);
		
		if (destID.compareTo(myID) == -1 && destID.compareTo(predID) == 1)
			return true;
		else
			return false; 
	}
	
	private void routeMessage(Message msg, BigInteger destination) {
		BigInteger oldDistance = RING_SIZE;
		BigInteger newDistance = RING_SIZE;
		Address address = null;
		BigInteger nextPeer = BigInteger.ZERO;
		
		if (pred != null && between(destination, pred.getPeerId(), myPeerAddress.getPeerId())) {
			// I am the rendezvous node
			System.out.println("*** Peer " + myPeerAddress.getPeerId() + " is the rendezvous node for " + destination);
		}
		else {
			// I am not the rendezvous node, route the message to the rendezvous node

			if (pred == null)
				System.err.println("Peer " + myPeerAddress.getPeerId() + ": pred is null.");
			
			// first, check in the succ list
			if (succ!= null) {
				//System.out.println("succ: " + succ.getPeerId() + " destination: " + destination);
				
				// peerID < topic =: finger ------ dest
				if (succ.getPeerId().compareTo(destination) == -1 
						|| succ.getPeerId().compareTo(destination) == 0) {
					newDistance = destination.subtract(succ.getPeerId());
				}
				// peerID > topic =: finger --- max --- dest
				else {
					newDistance = destination.subtract(succ.getPeerId()).add(RING_SIZE);							
				}
			}
			
			// newDistance < oldDisntace
			if (newDistance.compareTo(oldDistance) == -1) {
				oldDistance = newDistance;
				address = succ.getPeerAddress();
				nextPeer = succ.getPeerId();
			}
			
			// then, check in the fingers list
			for(int i = 0; i < fingers.length; i++) {
				
				if (newDistance.equals(BigInteger.ZERO))
					break;
				
				if (fingers[i] != null) {
					
					//System.out.println("fingers: " + fingers[i].getPeerId() + " destination: " + destination 
					//		+ " oldDistance " + oldDistance);
					
					// peerID < topic =: finger ------ dest
					if (fingers[i].getPeerId().compareTo(destination) == -1
							|| fingers[i].getPeerId().compareTo(destination) == 0) {
						newDistance = destination.subtract(fingers[i].getPeerId());
					}
					// peerID > topic =: finger --- max --- dest
					else {
						newDistance = destination.subtract(fingers[i].getPeerId()).add(RING_SIZE);							
					}
				}
				
				// newDistance < oldDisntace
				if (newDistance.compareTo(oldDistance) == -1) {
					oldDistance = newDistance;
					address = fingers[i].getPeerAddress();
					nextPeer = fingers[i].getPeerId();
				}
			}
		}
		
		if (address != null) { 
			System.out.println("Peer " + myPeerAddress.getPeerId() + " routed a message on id " + nextPeer + " "  + address);
			msg.setDestination(address);
			trigger(msg, network);
		}
	}
	
	// -------------------------------------------------------------------------
	private void sendSubscribeRequest(BigInteger topicID, BigInteger lastSequenceNum) {
		
		BigInteger hashedTopicID = BigInteger.valueOf(topicID.hashCode());
		SubscribeRequest sub = new SubscribeRequest(topicID, lastSequenceNum, myAddress, null);

		System.out.println("+ Peer " + myPeerAddress.getPeerId() + " is triggering a SubscribeRequest topicID: " +topicID + " hashed: " +hashedTopicID);

		routeMessage(sub, hashedTopicID);
	}
	
	private void sendUnsubscribeRequest(BigInteger topicID) {
		BigInteger hashedTopicID = BigInteger.valueOf(topicID.hashCode());
		UnsubscribeRequest unsub = new UnsubscribeRequest(topicID, myAddress, null);
		
		System.out.println("- Peer " + myPeerAddress.getPeerId() + " is triggering a UnsubscribeRequest topicID: " +topicID + " hashed: " +hashedTopicID);

		routeMessage(unsub, hashedTopicID);
	}
	
	private void publish(BigInteger topicID, String content) {
		System.out.println("Peer " + myPeerAddress.getPeerId() + " is publishing an event.");
		
		BigInteger hashedTopicID = BigInteger.valueOf(topicID.hashCode());
		trigger(new Publication(hashedTopicID, publicationSeqNum, content, myAddress, serverAddress), network);
		publicationSeqNum.add(BigInteger.ONE);
	}
	
	Handler<Start> handleStart = new Handler<Start>() {
		@Override
		public void handle(Start event) {
//			System.out.println("Peer -- inside the handleStart()");
			/*
			System.out.println("Peer " + myAddress.getId() + " is started.");
			Address add = new Address(myAddress.getIp(), myAddress.getPort(), myAddress.getId()-1);
			Notification notification = new Notification("test",
					"nothing", myAddress, myAddress);
			trigger(notification, network);
			String topic = "Football";
			sendSubscribeRequest(topic);
			
*/
			
	
			//sendUnsubscribeRequest(topic);
		}
	};
	
	Handler<SubscriptionInit> handleSubscriptionInit = new Handler<SubscriptionInit>() {
		@Override
		public void handle(SubscriptionInit si) {
			Vector<BigInteger> topicIDs = si.getTopicIDs();
			
			Iterator it = topicIDs.iterator();
			while(it.hasNext()) {
				BigInteger topicID = (BigInteger) it.next();			
				sendSubscribeRequest(topicID, BigInteger.ZERO);
				
			}
			

		}
	};


	
	Handler<SubscribePeer> handleSubscribe = new Handler<SubscribePeer>() {
		@Override
		public void handle(SubscribePeer event) {
			BigInteger topicID = event.getTopicID(); 
			
			BigInteger lastSequenceNumber = BigInteger.ZERO;
			if (mySubscriptions.containsKey(topicID))
				lastSequenceNumber = mySubscriptions.get(topicID);
			mySubscriptions.put(topicID, lastSequenceNumber);

			sendSubscribeRequest(topicID, lastSequenceNumber);
		}
	};
	
	Handler<UnsubscribePeer> handleUnsubscribe = new Handler<UnsubscribePeer>() {
		@Override
		public void handle(UnsubscribePeer event) {
			
			System.out.println("Peer " + myPeerAddress.getPeerId() + " is unsubscribing an event.");

			
			if (!mySubscriptions.isEmpty()) {
				Set<BigInteger> topicIDs = mySubscriptions.keySet(); // TODO: we can randomize later. randomization should be done in the simulation class.
				    Iterator<BigInteger> it = topicIDs.iterator();
					BigInteger topicID = it.next();
					
					// topicID should not be removed from the list, so that the next subscription can use the lastSequenceNumber
					// mySubscriptions.remove(topicID); 
					
					sendUnsubscribeRequest(topicID);
			}
		}
	};
	
	Handler<PublishPeer> handlePublish = new Handler<PublishPeer>() {
		@Override
		public void handle(PublishPeer event) {
			String info = "Test"; 
			//publish(TopicList.getRandomTopic(), info);	// Assumptions: we can publish something that we don't subscribe
			
			publish(myPeerAddress.getPeerId(), info);
		}
	};


	
//-------------------------------------------------------------------
// This handler is called periodically, every msgPeriod milliseconds.
//-------------------------------------------------------------------
	/*
	Handler<SendMessage> handleSendMessage = new Handler<SendMessage>() {
		@Override
		public void handle(SendMessage event) {
			sendMessage();
		}
	};
*/
//-------------------------------------------------------------------
// Whenever a node receives a PeerMessage from another node, this
// handler is triggered.
// In this handler the node, add the address of the sender and the
// address of another nodes, which has been sent by PeerMessage
// to its friend list, and updates its state in the Snapshot.
// The node registers the nodes added to its friend list and
// unregisters the node removed from the list.
//-------------------------------------------------------------------
	/*
	Handler<PeerMessage> handleRecvMessage = new Handler<PeerMessage>() {
		@Override
		public void handle(PeerMessage event) {
			PeerAddress oldFriend;
			PeerAddress sender = event.getMSPeerSource();
			PeerAddress newFriend = event.getNewFriend();

			// add the sender address to the list of friends
			if (!friends.contains(sender)) {
				if (friends.size() == viewSize) {
					oldFriend = friends.get(rand.nextInt(viewSize));
					friends.remove(oldFriend);
					fdUnregister(oldFriend);
					Snapshot.removeFriend(myPeerAddress, oldFriend);
				}

				friends.addElement(sender);
				fdRegister(sender);
				Snapshot.addFriend(myPeerAddress, sender);
			}

			// add the received new friend from the sender to the list of friends
			if (!friends.contains(newFriend) && !myPeerAddress.equals(newFriend)) {
				if (friends.size() == viewSize) {
					oldFriend = friends.get(rand.nextInt(viewSize));
					friends.remove(oldFriend);
					fdUnregister(oldFriend);
					Snapshot.removeFriend(myPeerAddress, oldFriend);
				}

				friends.addElement(newFriend);
				fdRegister(newFriend);
				Snapshot.addFriend(myPeerAddress, newFriend);				
			}			
		}
	};
*/
//-------------------------------------------------------------------
// In this method a node selects a random node, e.g. randomDest,
// and sends it the address of another random node from its friend
// list, e.g. randomFriend.
//-------------------------------------------------------------------
	/*
	private void sendMessage() {
		if (friends.size() == 0)
			return;
		
		PeerAddress randomDest = friends.get(rand.nextInt(friends.size()));
		PeerAddress randomFriend = friends.get(rand.nextInt(friends.size()));
		
		if (randomFriend != null)
			trigger(new PeerMessage(myPeerAddress, randomDest, randomFriend), network);
	}
	*/
//-------------------------------------------------------------------
// This method shows how to register the failure detector for a node.
//-------------------------------------------------------------------
	private void fdRegister(PeerAddress peer) {
		Address peerAddress = peer.getPeerAddress();
		StartProbingPeer spp = new StartProbingPeer(peerAddress, peer);
		fdRequests.put(peerAddress, spp.getRequestId());
		trigger(spp, fd.getPositive(FailureDetector.class));
		
		fdPeers.put(peerAddress, peer);
	}

//-------------------------------------------------------------------	
// This method shows how to unregister the failure detector for a node.
//-------------------------------------------------------------------
	private void fdUnregister(PeerAddress peer) {
		if (peer == null)
			return;
			
		Address peerAddress = peer.getPeerAddress();
		trigger(new StopProbingPeer(peerAddress, fdRequests.get(peerAddress)), fd.getPositive(FailureDetector.class));
		fdRequests.remove(peerAddress);
		
		fdPeers.remove(peerAddress);
	}
}
