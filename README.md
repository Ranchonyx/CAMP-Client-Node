# CAMP

CAMP is a small binary protocol for sending arbitrary messages, large data and real-time information over WebSocket.
It uses fixed frame types for acknowledgements, heartbeats, UTF-8 text, binary payloads and streamed transactions.

This is a client implementation of the CAMP protocol specified
at [CAMP-Protocol](https://github.com/Ranchonyx/CAMP-Protocol)

## CAMP-Client-Node / Overview

The CAMP Node.Js client takes care of the following:

- Authentication at the server
- Correct framing and structuring of received and sent data

The client provides a public API for creating and destroying an instance.
It provides access incoming communication via events

## Setup

To set up a CAMP Client, simply import the `camp` function from the ``camp-client-node`` package.

The `CAMP`-function takes two arguments:

- host
    - a required host string
- bearer
    - a required authentication token string
- timeout
    - an optional timeout value, indicating how long the client should wait until a connection request to the server is
      aborted

## CAMPClientWebsocketSession / Overview

### Public methods

| Name  | Parameter      | Description                   | Returns |
|-------|----------------|-------------------------------|---------|
| Close | reason: string | Closes the session gracefully |         |

CAMP-Functionality is divided among namespaces as defined in ``CAMP``.

#### Base

Accessible via ``session.base.<method>(...);``

| Name       | Parameter       | Description                              | Returns |
|------------|-----------------|------------------------------------------|---------|
| Ping       |                 | Pings the client                         |         |
| SendUTF8   | data: string    | Sends the passed utf8 text to the client |         |
| SendBinary | data: Buffer    | Sends the passed buffer to the client    |         |
| SendError  | message: string | Send an error to the client              |         |

#### Transaction

Accessible via ``session.transaction.<method>(...);``

| Name               | Parameter                                                                         | Description                                                 | Returns                 |
|--------------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------|-------------------------|
| Stream             | source: Readable, options: { streamName: string, behaviour: CAMP_FLOW_BEHAVIOUR } | Streams a readable to the client                            |                         |
| WaitForStream      | name: string, timeout: number                                                     | Waits for a named stream from client                        | Promise\<CAMPReadable\> |
| StreamRequestRange | stream: CAMPReadable, start: bigint, end: bigint                                  | When in TX_PULL-mode, requests a byte range from the client |                         |
| StreamCancel       | stream: CAMPReadable                                                              | Cancels a Stream                                            |                         |

### Data Events

These events are emitted when the server-side session receives data from a client-side session

| Name           | Parameter    | Description                                                    |
|----------------|--------------|----------------------------------------------------------------|
| message-utf8   | string       | Emitted, when the session receives a utf8 text message         |
| message-binary | CAMPBuffer   | Emitted, when the session receives an arbitrary binary message |
| message-error  | data: string | Emitted, when the session receives an error message            |

### Meta events

This category of events is emitted when the session state changes

| Name         | Parameter                      | Description                                     |
|--------------|--------------------------------|-------------------------------------------------|
| connected    |                                | Emitted, when the session successfully connects |
| disconnected |                                | Emitted, when the session has been disconnected |
| reconnected  |                                | Emitted, when the session has reconnected       |
| closed       | [code: number, reason: string] | Emitted, when the session is closed             |

## CAMP-Client / Example

```typescript
import {CAMP} from "camp-client-node";

const HOST = "localhost:8080";
const TOKEN = "SOME_AUTH_TOKEN";

const client = await CAMP(HOST, TOKEN, 10000);
client.on("connected", () => {
    console.info(`Successfully connected to ${HOST}`);
});
```