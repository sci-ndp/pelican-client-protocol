Message Queue Application
=========================

Create a `cmd/messagequeue` application with the following spec:

Creates an application built on top of a stomp server, as in cmd/helloworld

Listens for subscriptions from new clients. Assume each client will set a unique `subscription` header in its SUBSCRIBE stomp frame
- Upon subscription, creates an in-memory event queue for each client

On application startup, the application listens for events from an EventSource interface
- provide a test implementation of this interface that generates an incrementing "Event <num>" string every 5 seconds.
- While there are no event queues registered, just drop new events
- If there are one or more event queues registered, put the event into each queue

Continually re-send the oldest event of each event queue as a MESSAGE to its relevant subscription
- Start with a 1 second delay, multiply delay by 1.2x each time
- After 16 retries, give up and disconnect the client.

If a client's event queue exceeds 100 messages, give up and disconnect the client.

Keep the event queue for a `subscription` header in-memory even if a client disconnects.
When a client re-subscribes, if its `subscription` header matches an existing in-memory queue, start re-sending from that queue
rather than creating a new queue.

Add new SockServer interface methods as needed to support this (particularly, OnAck)
