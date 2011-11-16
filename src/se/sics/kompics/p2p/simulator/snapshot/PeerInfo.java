package se.sics.kompics.p2p.simulator.snapshot;

import p2p.system.peer.Peer;
import p2p.system.peer.PeerAddress;


public class PeerInfo {
	private PeerAddress self;	
	private PeerAddress pred;
	private PeerAddress succ;
	private PeerAddress[] fingers = new PeerAddress[Peer.FINGER_SIZE];
	private PeerAddress[] succList = new PeerAddress[Peer.SUCC_SIZE];

//-------------------------------------------------------------------
	public PeerInfo(PeerAddress self) {
		this.self = self;
	}

//-------------------------------------------------------------------
	public void setPred(PeerAddress pred) {
		this.pred = pred;
	}

//-------------------------------------------------------------------
	public void setSucc(PeerAddress succ) {
		this.succ = succ;
	}

//-------------------------------------------------------------------
	public void setFingers(PeerAddress[] fingers) {
		for (int i = 0; i < fingers.length; i++)
		this.fingers[i] = fingers[i];
	}

//-------------------------------------------------------------------
	public void setSuccList(PeerAddress[] succList) {
		for (int i = 0; i < succList.length; i++)
		this.succList[i] = succList[i];
	}

//-------------------------------------------------------------------
	public PeerAddress getSelf() {
		return this.self;
	}

//-------------------------------------------------------------------
	public PeerAddress getPred() {
		return this.pred;
	}

//-------------------------------------------------------------------
	public PeerAddress getSucc() {
		return this.succ;
	}

//-------------------------------------------------------------------
	public PeerAddress[] getFingers() {
		return this.fingers;
	}

//-------------------------------------------------------------------
	public PeerAddress[] getSuccList() {
		return this.succList;
	}

//-------------------------------------------------------------------
	public String toString() {
		String str = new String();
		String finger = new String();
		String succs = new String();
		
		finger = "[";
		for (int i = 0; i < Peer.FINGER_SIZE; i++)
			finger += this.fingers[i] + ", ";
		finger += "]";

		succs = "[";
		for (int i = 0; i < Peer.SUCC_SIZE; i++)
			succs += this.succList[i] + ", ";
		succs += "]";

		str += "peer: " + this.self;
		str += ", succ: " + this.succ;
		str += ", pred: " + this.pred;
		str += ", fingers: " + finger;
		str += ", succList: " + succs;
		
		return str;
	}

}
