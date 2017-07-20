 /**e********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

//Forward Decl
void* prepareJoinReqMsg(Address* addrPtr, long* heartBeat, size_t* msgSize);
void* prepareJoinRepMsg(Address* addrPtr, long* heartBeat, vector<MemberListEntry>& memberList, size_t* msgSize);
int getAddressId(Address* addr);
short getAddressPort(Address* addr);

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	this->isIntroducer = false;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	initMemberListTable(memberNode);
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) { //Introducer
        
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Introducer: Starting up the group...");
#endif
        isIntroducer = true;
        memberNode->inGroup = true;
    }
    
    else {  //Peer
        size_t msgSize(0);
        MessageHdr *msg = (MessageHdr*)prepareJoinReqMsg(&memberNode->addr, &memberNode->heartbeat, &msgSize);

#ifdef DEBUGLOG
        sprintf(s, "Peer: Trying to join the group...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgSize);

        free(msg);
    }

    return 1;
}



/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
   
    //Parse received message (Boiler-Plate)
    MessageHdr *msg = (MessageHdr*) data;
    MsgTypes msgType = msg->msgType;
    
    //--------------- Custom Protocol Implementation Begins --------------- 
    if(this->isIntroducer && msg->msgType == JOINREQ) {
        //JoinREQ message received by introducer, respond with JoinREP
        processJoinReqMsg(env, data, size);        
    }
    else if(!this->isIntroducer && msg->msgType == JOINREP) {
        //JoinREP message received by peer, update memberlist
        processJoinRepMsg(env, data, size);
    }
    else if(msg->msgType == HEARTBEAT) {
        processHeartbeatMsg(env, data, size);
    }
    else {
        log->LOG(&memberNode->addr, "Illegal State: Unexpected messages received!");
    }
    
    //Free memory after processing the received message
    free(data); 
    return true;
}

void MP1Node::processJoinReqMsg(void *env, char *data, int size) {
    //Parse joinREQ message (Boiler-Plate)
    MessageHdr *msg = (MessageHdr*) data;
    MsgTypes msgType = msg->msgType;
    Address* addressPtr = new Address();
    memcpy(addressPtr->addr, (char*)(msg + 1), sizeof(Address));
    long* heartBeat = (long *)((char *)(msg+1) + 1 + sizeof(Address));
    Member* currMemberNode = (Member*)env;                                  //Equal to "memberNode"
    
#ifdef DEBUGLOG
    static char s[1024];
    sprintf(s, "Message received\t Type: %d\tFrom: %d.%d.%d.%d:%d\tHeartbeat: %ld", 
        msg->msgType, addressPtr->addr[0],addressPtr->addr[1],addressPtr->addr[2], addressPtr->addr[3], 
        *(short*)&addressPtr->addr[4], *heartBeat);  
    log->LOG(&memberNode->addr, s);
#endif

    //Respond with JoinREP message
    size_t replyMsgSize = 0;
    MessageHdr* replyMsg = (MessageHdr*)prepareJoinRepMsg(&memberNode->addr, &memberNode->heartbeat, currMemberNode->memberList, &replyMsgSize);
    emulNet->ENsend(&memberNode->addr, addressPtr, (char *)replyMsg, replyMsgSize);
    
    //Add peer to membership list
    unsigned long currentTime = (unsigned long) par->getcurrtime();
    MemberListEntry newPeer = MemberListEntry(getAddressId(addressPtr), getAddressPort(addressPtr), *heartBeat, currentTime);
    currMemberNode->memberList.push_back(newPeer);
    log->logNodeAdd(&memberNode->addr, addressPtr);
    
    //Free allocated memory
    delete addressPtr;
    free(replyMsg);
}

void MP1Node::processJoinRepMsg(void *env, char *data, int size) {
    //Parse joinREP message
    MessageHdr *msg = (MessageHdr*) data;
    MsgTypes msgType = msg->msgType;
    Address* addressPtr = new Address();
    memcpy(addressPtr->addr, (char*)(msg + 1), sizeof(Address));
    long* heartBeat = (long *)((char *)(msg+1) + 1 + sizeof(Address));
    int* memberListSize = (int *)((char *)(msg+1) + 1 + sizeof(Address) + sizeof(long)); 
            
    vector<MemberListEntry> peerMemberList;
    peerMemberList.resize(*memberListSize);
    char* memberListBuffer = (char*)((char *)(msg+1) + 1 + sizeof(Address) + sizeof(long) + sizeof(int));
    copy(memberListBuffer, memberListBuffer + (*memberListSize) * sizeof(MemberListEntry), reinterpret_cast<char *>(peerMemberList.data()));
     
    Member* currMemberNode = (Member*)env;                                  //Equal to "memberNode"
    
#ifdef DEBUGLOG
    static char s[1024];
    sprintf(s, "Message received\t Type: %d\tFrom: %d.%d.%d.%d:%d\tHeartbeat: %ld\tPeerListSize: %d", 
        msg->msgType, addressPtr->addr[0],addressPtr->addr[1],addressPtr->addr[2], addressPtr->addr[3], 
        *(short*)&addressPtr->addr[4], *heartBeat, peerMemberList.size());
    log->LOG(&memberNode->addr, s);
#endif
        
    //printMemberList(peerMemberList);

    //Add peers to the membership list
    memberNode->memberList.insert(memberNode->memberList.end(), peerMemberList.begin(), peerMemberList.end());
    for(const MemberListEntry& entry : peerMemberList) {
        static char s[1024];
        sprintf(s, "%d.%d.%d.%d:%d", entry.id & 0xFF, (entry.id >> 8) & 0xFF, (entry.id >> 16) & 0xFF, (entry.id >> 24) & 0xFF, 
            *(short*)&entry.port);
        Address* peerAddr = new Address(string(s));
        log->logNodeAdd(&memberNode->addr, peerAddr);
        delete peerAddr;
    }
        
    printMemberList(memberNode->memberList);
    
    //mark self as part of the group
    memberNode->inGroup = true;
    
    //Cleanup
    delete addressPtr;
}

void MP1Node::processHeartbeatMsg(void *env, char *data, int size) {
    //Compare currentTime/timestamp with Tfail
    //    update membership list (processPeerMemberList)
    //    Or, ignore
}

void MP1Node::processPeerMemberList(const vector<MemberListEntry>& peerMemberList) {
    
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list (heartbeat)
 */
void MP1Node::nodeLoopOps() {
    //update own heartbeat in membership list
    updateOwnHeartbeat();
    
    //Cleanup members which have not responded since TREMOVE
    removeStaleMembers();
    
    //select b random valid nodes from membership list (other than self)
    vector<MemberListEntry> randomNodes = selectFanoutRandomPeers();
    
    
//        send hearbeat
//                Filter out failed members (Tfail comparison here)

    return;
}

//TODO Do not include nodes which have crossed TFail
vector<MemberListEntry> MP1Node::selectFanoutRandomPeers() {
    vector<MemberListEntry> randomNodes;
    
    int i = GOSSIP_FANOUT;
    while(i-- && memberNode->memberList.size() > 1) {                   //Check if peer exists
        int index = rand() % (memberNode->memberList.size()-1) + 1;     //Random number between 1 and N-1
        const MemberListEntry& selectedNode = memberNode->memberList[index];
        randomNodes.push_back(selectedNode);
        
#ifdef DEBUGLOG2
        static char s[1024];
        sprintf(s, "%d.%d.%d.%d:%d", selectedNode.id & 0xFF, (selectedNode.id >> 8) & 0xFF, (selectedNode.id >> 16) & 0xFF, (selectedNode.id >> 24) & 0xFF, 
            *(short*)&selectedNode.port);
        string peerAddressStr(s);
        sprintf(s, ("Fanout Random Member - Address: " + peerAddressStr + "\tHeartbeat: %ld\tTimestamp: %ld").c_str(), selectedNode.heartbeat, selectedNode.timestamp);
        log->LOG(&memberNode->addr, s);
#endif            

    }
    
    return randomNodes;
}

void MP1Node::removeStaleMembers() {
    static char s[1024];
    unsigned long currentTime = (unsigned long) this->par->getcurrtime();
    
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    //it++;
    for ( ; it != memberNode->memberList.end(); ) {
        
        if ((currentTime - (unsigned long)it->timestamp) >= (unsigned long)TREMOVE) {
            sprintf(s, "%d.%d.%d.%d:%d", it->id & 0xFF, (it->id >> 8) & 0xFF, (it->id >> 16) & 0xFF, (it->id >> 24) & 0xFF, 
                *(short*)&it->port);
            string peerAddressStr = string(s);
            Address peerAddr(peerAddressStr);
            
#ifdef DEBUGLOG
            sprintf(s, ("Member Removed - Address: " + peerAddressStr + "\tHeartbeat: %ld\tTimestamp: %ld").c_str(), it->heartbeat, it->timestamp);
            log->LOG(&memberNode->addr, s);
#endif    

            log->logNodeRemove(&memberNode->addr, &peerAddr);
            it = memberNode->memberList.erase(it);
        } 
        else { ++it; }
    }
}

void MP1Node::updateOwnHeartbeat() {
    memberNode->heartbeat++;
    memberNode->myPos->heartbeat++;
    memberNode->myPos->settimestamp(par->getcurrtime());
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list. Add self to the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
 
    unsigned long currentTime = (unsigned long) par->getcurrtime();
    MemberListEntry self = MemberListEntry(getAddressId(&memberNode->addr), getAddressPort(&memberNode->addr), memberNode->heartbeat, currentTime);
    
    //First element in the membership list is the current node
    memberNode->memberList.push_back(self);
    memberNode->myPos = memberNode->memberList.begin();
}

void MP1Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2], addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::printMemberList(const vector<MemberListEntry>& memberList) {
#ifdef DEBUGLOG
    static char s[1024];
    for(int i = 0; i < memberList.size(); i++) {
        MemberListEntry entry = memberList[i];
        sprintf(s, "MemberList %d: Address: %d.%d.%d.%d:%d\tHeartbeat: %ld\tTimestamp: %ld", i, 
            entry.id & 0xFF, (entry.id >> 8) & 0xFF, (entry.id >> 16) & 0xFF, (entry.id >> 24) & 0xFF, 
            *(short*)&entry.port, entry.heartbeat, entry.timestamp);
        log->LOG(&memberNode->addr, s);
    }
    
    sprintf(s, "");
    log->LOG(&memberNode->addr, s);
#endif
}

void MP1Node::printMemberList(vector<MemberListEntry>::const_iterator begin, vector<MemberListEntry>::const_iterator end) {
#ifdef DEBUGLOG
    static char s[1024];
    while(begin != end) {
        sprintf(s, "MemberList Address: %d.%d.%d.%d:%d\tHeartbeat: %ld\tTimestamp: %ld", 
            begin->id & 0xFF, (begin->id >> 8) & 0xFF, (begin->id >> 16) & 0xFF, (begin->id >> 24) & 0xFF, 
            *(short*)&begin->port, begin->heartbeat, begin->timestamp);
        log->LOG(&memberNode->addr, s);
        begin++;
    }    
    sprintf(s, "");
    log->LOG(&memberNode->addr, s);
#endif
}

void* prepareJoinReqMsg(Address* addrPtr, long* heartBeat, size_t* msgSize) {
    *msgSize = sizeof(MessageHdr) + sizeof(*addrPtr) + sizeof(long) + 1;
    MessageHdr* msg = (MessageHdr *) malloc((*msgSize) * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *)(msg+1), &addrPtr->addr, sizeof(addrPtr->addr));
    memcpy((char *)(msg+1) + 1 + sizeof(addrPtr->addr), heartBeat, sizeof(long));

    return (void*)msg;
}

void* prepareJoinRepMsg(Address* addrPtr, long* heartBeat, vector<MemberListEntry>& memberList, size_t* msgSize) {
    size_t memberListSize = memberList.size() * sizeof(MemberListEntry);
    *msgSize = sizeof(MessageHdr) + sizeof(*addrPtr) + sizeof(long) + memberListSize + sizeof(int) + 1;
    MessageHdr* msg = (MessageHdr *) malloc((*msgSize) * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), &addrPtr->addr, sizeof(addrPtr->addr));
    memcpy((char *)(msg+1) + 1 + sizeof(*addrPtr), heartBeat, sizeof(long));
    
    int memberListCount = memberList.size();
    memcpy((char *)(msg+1) + 1 + sizeof(*addrPtr) + sizeof(long), &memberListCount, sizeof(int));
    
    http://ideone.com/7H8dy
    copy( reinterpret_cast<char *>(memberList.data()), reinterpret_cast<char *>(memberList.data()) + memberListSize, (char *)(msg+1) + 1 + sizeof(*addrPtr) + sizeof(long) + sizeof(int));          

    return (void*)msg;
}

int getAddressId(Address* addr) {
    int id = 0;
    memcpy(&id, &addr->addr[0], sizeof(int));
    return id;
}

short getAddressPort(Address* addr) {
    short port;
    memcpy(&port, &addr->addr[4], sizeof(short));
}