package centralized.simulator.scenarios;


import java.math.BigInteger;

import centralized.simulator.core.event.AllPeerSubscribe;
import centralized.simulator.core.event.PeerFail;
import centralized.simulator.core.event.PeerJoin;
import centralized.simulator.core.event.PeerPublish;
import centralized.simulator.core.event.PeerSubscribe;
import centralized.simulator.core.event.PeerUnsubscribe;
import centralized.simulator.core.event.ServerStart;

import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;

@SuppressWarnings("serial")
public class Operations {

//-------------------------------------------------------------------
	static Operation1<PeerJoin, BigInteger> peerJoin = new Operation1<PeerJoin, BigInteger>() {
			public PeerJoin generate(BigInteger id) {
				return new PeerJoin(id);
			}
		};
	
//-------------------------------------------------------------------
	static Operation1<PeerFail, BigInteger> peerFail = new Operation1<PeerFail, BigInteger>() {
		public PeerFail generate(BigInteger id) {
			return new PeerFail(id);
		}
	};
	
//-------------------------------------------------------------------
	static Operation1<ServerStart, BigInteger> serverStart = new Operation1<ServerStart, BigInteger>() {
			public ServerStart generate(BigInteger id) {
				return new ServerStart(id);
			}
		};
		
//-------------------------------------------------------------------
	static Operation1<PeerSubscribe, BigInteger> peerSubscribe = new Operation1<PeerSubscribe, BigInteger>() {
			public PeerSubscribe generate(BigInteger id) {
				return new PeerSubscribe(id);
			}
		};
		
//-------------------------------------------------------------------
	static Operation<AllPeerSubscribe> allPeerSubscribe = new Operation<AllPeerSubscribe>() {
			public AllPeerSubscribe generate() {
				return new AllPeerSubscribe();
			}
		};
		
//-------------------------------------------------------------------
	static Operation1<PeerUnsubscribe, BigInteger> peerUnsubscribe = new Operation1<PeerUnsubscribe, BigInteger>() {
			public PeerUnsubscribe generate(BigInteger id) {
				return new PeerUnsubscribe(id);
			}
		};
		
//-------------------------------------------------------------------
	static Operation1<PeerPublish, BigInteger> peerPublish = new Operation1<PeerPublish, BigInteger>() {
			public PeerPublish generate(BigInteger id) {
				return new PeerPublish(id);
			}
		};
}
