package centralized.simulator.core;

import centralized.simulator.core.event.AllPeerSubscribe;
import centralized.simulator.core.event.PeerFail;
import centralized.simulator.core.event.PeerJoin;
import centralized.simulator.core.event.PeerPublish;
import centralized.simulator.core.event.PeerSubscribe;
import centralized.simulator.core.event.PeerUnsubscribe;
import centralized.simulator.core.event.ServerStart;
import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public class SimulatorPort extends PortType {{
	positive(PeerJoin.class);
	positive(ServerStart.class);
	positive(PeerFail.class);	
	positive(PeerSubscribe.class);
	positive(PeerUnsubscribe.class);
	positive(PeerPublish.class);
	positive(AllPeerSubscribe.class);
	negative(TerminateExperiment.class);
}}
