/**********************************
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
        
        //Parse joinREQ message
        Address* addressPtr = new Address();
        memcpy(addressPtr->addr, (char*)(msg + 1), sizeof(Address));
        long* heartBeat = (long *)((char *)(msg+1) + 1 + sizeof(Address));
        Member* currMemberNode = (Member*)env;                                  //Equal to "memberNode"
    
#ifdef DEBUGLOG
        static char s[1024];
        sprintf(s, "Message received\t Type: %d\tFrom: %d.%d.%d.%d:%d\tHeartbeat: %ld\tInited: %d\tInGroup: %d", 
            msg->msgType, addressPtr->addr[0],addressPtr->addr[1],addressPtr->addr[2], addressPtr->addr[3], 
            *(short*)&addressPtr->addr[4], *heartBeat, currMemberNode->inited, currMemberNode->inGroup);  
        log->LOG(&memberNode->addr, s);
#endif

        //Respond with JoinREP message
        size_t replyMsgSize = 0;
        MessageHdr* replyMsg = (MessageHdr*)prepareJoinRepMsg(&memberNode->addr, &memberNode->heartbeat, currMemberNode->memberList, &replyMsgSize);
        emulNet->ENsend(&memberNode->addr, addressPtr, (char *)replyMsg, replyMsgSize);
        
        //Add peer to membership list
        unsigned long currentTime = time(NULL);
        MemberListEntry newPeer = MemberListEntry(getAddressId(addressPtr), getAddressPort(addressPtr), *heartBeat, currentTime);
        currMemberNode->memberList.push_back(newPeer);
        
        //Free allocated memory
        free(addressPtr);
        free(replyMsg);
    }
    else if(!this->isIntroducer && msg->msgType == JOINREP) {
        
        //Parse joinREP message
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
        sprintf(s, "Message received\t Type: %d\tFrom: %d.%d.%d.%d:%d\tHeartbeat: %ld\tInited: %d\tInGroup: %d\tPeerListSize: %d", 
            msg->msgType, addressPtr->addr[0],addressPtr->addr[1],addressPtr->addr[2], addressPtr->addr[3], 
            *(short*)&addressPtr->addr[4], *heartBeat, currMemberNode->inited, currMemberNode->inGroup, peerMemberList.size());
        log->LOG(&memberNode->addr, s);
        
        //Print membership list of peer
        for(int i = 0; i < peerMemberList.size(); i++) {
            MemberListEntry entry = peerMemberList[i];
            sprintf(s, "Member %d: Address: %d.%d.%d.%d:%d\tHeartbeat: %ld\tTimestamp: %ld", i, 
                entry.id & 0xFF, (entry.id >> 8) & 0xFF, (entry.id >> 16) & 0xFF, (entry.id >> 24) & 0xFF, 
                *(short*)&entry.port, entry.heartbeat, entry.timestamp);
            log->LOG(&memberNode->addr, s);
        }
#endif
    }
    else {
        log->LOG(&memberNode->addr, "Illegal State: Unexpected messages received!");
    }
    
    //TODO: Who frees memory in case of receive message?
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    return;
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
 
    unsigned long currentTime = time(NULL);
    MemberListEntry self = MemberListEntry(getAddressId(&memberNode->addr), getAddressPort(&memberNode->addr), memberNode->heartbeat, currentTime);
    
    //First element in the membership list is the current node
    memberNode->memberList.push_back(self);
    memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
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