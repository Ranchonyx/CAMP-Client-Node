# Cryo

Cryo is a small binary protocol for sending arbitrary messages, large data and real-time information over WebSocket.
It uses fixed frame types for acknowledgements, heartbeats, UTF-8 text, binary payloads and streamed transactions.

This is an implementation of the Cryo protocol specified at [Cryo-Protocol](https://github.com/Ranchonyx/Cryo-Protocol)

## Cryo-Client-Node / Overview

The Cryo Node.js client takes care of the following:

- Authentication at the server
- Correct framing and structuring of received and sent data

The client provides a public API for creating and destroying an instance.
It provides access incoming communication via events

## Setup

To set up a Cryo Client, simply import the `cryo` function from the ``cryo-client-node`` package.

The `cryo`-function takes two arguments:

- host
    - a required host string
- bearer
    - a required authentication token string
- timeout
    - an optional timeout value, indicating how long the client should wait until a connection request to the server is
      aborted

## CryoClientWebsocketSession / Overview

### Public methods

| Name                   | Parameter                              | Description                                | Returns           |
|------------------------|----------------------------------------|--------------------------------------------|-------------------|
| SendUTF8               | message: string                        | Streams a Readable to the server           |                   |
| SendBinary             | message: Buffer                        | Streams a Readable to the server           |                   |
| Stream                 | source: Readable, name?: string        | Streams a Readable to the server           |                   |
| WaitForStream          | streamName?: string , timeout?: number | Waits for a named stream                   | Promise<Readable> |
| SetIncomingFlowControl | behaviour: TX_PULL \| TX_PUSH          | Sets the remote flow control               |                   |   
| Close                  |                                        | Closes the underlying Websocket connection |                   |
| Destroy                | code?: number, message?: string        | Tears down the session                     |                   |

### Data Events

These events are emitted when the server-side session receives data from a client-side session

| Name           | Parameter    | Description                                                    |
|----------------|--------------|----------------------------------------------------------------|
| message-utf8   | data: string | Emitted, when the session receives a utf8 text message         |
| message-binary | data: Buffer | Emitted, when the session receives an arbitrary binary message |

### Meta events

This category of events is emitted when the session state changes

| Name         | Parameter                    | Description                                     |
|--------------|------------------------------|-------------------------------------------------|
| connected    |                              | Emitted, when the session successfully connects |
| disconnected |                              | Emitted, when the session has been disconnected |
| reconnected  |                              | Emitted, when the session has reconnected       |
| closed       | code: number, reason: string | Emitted, when the session is closed             |

## Cryo-Client-Node / Example

```typescript
import {cryo} from "cryo-client-node";

const HOST = "localhost:8080";
const TOKEN = process.env.CRYO_AUTH_TOKEN;

const client = await cryo(HOST, TOKEN, false, 10000);
client.on("connected", () => {
    console.info(`Successfully connected to ${HOST}`);
});
```