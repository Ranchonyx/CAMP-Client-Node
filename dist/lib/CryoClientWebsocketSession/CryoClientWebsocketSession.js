import EventEmitter from "node:events";
import { AckTracker } from "../Common/AckTracker/AckTracker.js";
import { CryoFrameInspector } from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import { CreateDebugLogger } from "../Common/Util/CreateDebugLogger.js";
import { CryoConnectionHelper } from "./CryoConnectionHelper.js";
import { Readable } from "node:stream";
import { ACKFrame, BinaryDataFrame, BinaryMessageType, BufferUtil, ByeFrame, CRYO_FLOW_BEHAVIOUR, CRYO_PROTOCOL_VERSION, cryoNewId, EndpointInfoFrame, ErrorFrame, PingPongFrame, TXChunkFrame, TXFetchFrame, TXFinishFrame, TXFlowFrame, TXStartFrame, Utf8DataFrame } from "cryo-protocol";
var CryoCloseCode;
(function (CryoCloseCode) {
    CryoCloseCode[CryoCloseCode["CLOSE_GRACEFUL"] = 4000] = "CLOSE_GRACEFUL";
    CryoCloseCode[CryoCloseCode["CLOSE_CLIENT_ERROR"] = 4001] = "CLOSE_CLIENT_ERROR";
    CryoCloseCode[CryoCloseCode["CLOSE_SERVER_ERROR"] = 4002] = "CLOSE_SERVER_ERROR";
})(CryoCloseCode || (CryoCloseCode = {}));
var WebsocketCloseCode;
(function (WebsocketCloseCode) {
    WebsocketCloseCode[WebsocketCloseCode["NORMAL"] = 1000] = "NORMAL";
    WebsocketCloseCode[WebsocketCloseCode["GOING_AWAY"] = 1001] = "GOING_AWAY";
    WebsocketCloseCode[WebsocketCloseCode["PROTOCOL_ERROR"] = 1002] = "PROTOCOL_ERROR";
    WebsocketCloseCode[WebsocketCloseCode["UNSUPPORTED_DATA"] = 1003] = "UNSUPPORTED_DATA";
    WebsocketCloseCode[WebsocketCloseCode["ABNORMAL_CLOSURE"] = 1006] = "ABNORMAL_CLOSURE";
    WebsocketCloseCode[WebsocketCloseCode["INVALID_PAYLOAD"] = 1007] = "INVALID_PAYLOAD";
    WebsocketCloseCode[WebsocketCloseCode["POLICY_VIOLATION"] = 1008] = "POLICY_VIOLATION";
    WebsocketCloseCode[WebsocketCloseCode["MESSAGE_TOO_BIG"] = 1009] = "MESSAGE_TOO_BIG";
    WebsocketCloseCode[WebsocketCloseCode["INTERNAL_ERROR"] = 1011] = "INTERNAL_ERROR";
    WebsocketCloseCode[WebsocketCloseCode["SERVICE_RESTART"] = 1012] = "SERVICE_RESTART";
    WebsocketCloseCode[WebsocketCloseCode["TRY_AGAIN_LATER"] = 1013] = "TRY_AGAIN_LATER";
    WebsocketCloseCode[WebsocketCloseCode["TLS_HANDSHAKE_FAILED"] = 1015] = "TLS_HANDSHAKE_FAILED";
})(WebsocketCloseCode || (WebsocketCloseCode = {}));
const DoReconnect = [WebsocketCloseCode.NORMAL, WebsocketCloseCode.GOING_AWAY, CryoCloseCode.CLOSE_GRACEFUL];
/*
* Cryo Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CryoClientWebsocketSession extends EventEmitter {
    socket;
    connectionHelper;
    sid;
    log;
    server_ack_tracker = new AckTracker();
    streams = new Map();
    bytes_rx = 0;
    bytes_tx = 0;
    current_ack = 0;
    current_txid = 0;
    receivedProtocolFeatures = 0n;
    outgoingFlowControl = CRYO_FLOW_BEHAVIOUR.TX_PUSH;
    constructor(socket, connectionHelper, sid, log = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();
        this.socket = socket;
        this.connectionHelper = connectionHelper;
        this.sid = sid;
        this.log = log;
        this.AttachListenersToSocket(socket);
        setImmediate(() => this.emit("connected"));
    }
    AttachListenersToSocket(socket) {
        socket.on("message", async (frame) => {
            await this.routeFrame(frame);
        });
        socket.on("error", this.HandleError.bind(this));
        socket.on("close", this.HandleClose.bind(this));
    }
    static async Connect(host, bearer, additionalQueryParamsMap, timeout = 5000, maxPayload = 256 * 1024 * 1024) {
        const sid = cryoNewId();
        const connHelper = new CryoConnectionHelper(host, bearer, sid, timeout, maxPayload, additionalQueryParamsMap);
        const socket = await connHelper.Acquire();
        return new CryoClientWebsocketSession(socket, connHelper, sid);
    }
    /**
     * Route a frame of any kind to its corresponding handler
     * */
    async routeFrame(frame) {
        const type = BufferUtil.GetType(frame);
        this.bytes_rx += frame.byteLength;
        try {
            this.log(`IN ${CryoFrameInspector.Inspect(frame)}`);
        }
        catch {
            this.log(`IN <INVALID MESSAGE>`);
        }
        switch (type) {
            case BinaryMessageType.PING_PONG:
                await this.HandlePingPongMessage(frame);
                return;
            case BinaryMessageType.ERROR:
                await this.HandleErrorMessage(frame);
                return;
            case BinaryMessageType.ACK:
                await this.HandleAckMessage(frame);
                return;
            case BinaryMessageType.UTF8DATA:
                await this.HandleUTF8DataMessage(frame);
                return;
            case BinaryMessageType.BINARYDATA:
                await this.HandleBinaryDataMessage(frame);
                return;
            case BinaryMessageType.TX_START:
                await this.HandleTxStartMessage(frame);
                return;
            case BinaryMessageType.TX_CHUNK:
                await this.HandleTxChunkMessage(frame);
                return;
            case BinaryMessageType.TX_FINISH:
                await this.HandleTxFinishMessage(frame);
                return;
            case BinaryMessageType.ENDPOINT_INFO:
                await this.HandleEndpointInfoMessage(frame);
                return;
            case BinaryMessageType.BYE:
                await this.HandleByeMessage(frame);
                return;
            case BinaryMessageType.TX_FLOW:
                await this.HandleTxFlowMessage(frame);
                return;
            case BinaryMessageType.TX_FETCH:
                await this.HandleTxFetchMessage(frame);
                return;
            default:
                this.log(`Unsupported binary message type ${type}!`);
        }
    }
    /*
    * Handle an outgoing binary message
    * */
    Send(outgoing_message) {
        //Create a pending message with a new ack number and queue it for acknowledgement by the server
        const type = BufferUtil.GetType(outgoing_message);
        if (type === BinaryMessageType.UTF8DATA || type === BinaryMessageType.BINARYDATA) {
            const message_ack = BufferUtil.GetAck(outgoing_message);
            this.server_ack_tracker.Track(message_ack, {
                timestamp: Date.now(),
                message: outgoing_message
            });
        }
        //Send the message buffer to the server
        if (!this.socket)
            return;
        this.socket.send(outgoing_message, (maybe_error) => {
            if (maybe_error)
                this.HandleError(maybe_error).then(r => null);
            else
                this.bytes_tx += outgoing_message.byteLength;
        });
        this.log(`Sent ${CryoFrameInspector.Inspect(outgoing_message)} to server.`);
    }
    /*
    * Respond to PONG frames with PING and vice versa
    * */
    async HandlePingPongMessage(message) {
        const decodedPingPongMessage = PingPongFrame
            .Deserialize(message);
        const ping_pongMessage = PingPongFrame
            .Serialize(this.sid, decodedPingPongMessage.ack, decodedPingPongMessage.payload === "pong" ? "ping" : "pong");
        this.Send(ping_pongMessage);
    }
    /*
    * Handling of binary error messages from the server, currently just log it
    * */
    async HandleErrorMessage(message) {
        const decodedErrorMessage = ErrorFrame
            .Deserialize(message);
        this.log(decodedErrorMessage.payload);
    }
    /*
    * Locally ACK the pending message if it matches the server's ACK
    * */
    async HandleAckMessage(message) {
        const decodedAckMessage = ACKFrame
            .Deserialize(message);
        const ack_id = decodedAckMessage.ack;
        const found_message = this.server_ack_tracker.Confirm(ack_id);
        if (!found_message) {
            this.log(`Got unknown ack_id ${ack_id} from server.`);
            return;
        }
        this.log(`Got ACK ${ack_id} from server.`);
    }
    /*
    * Extract payload from the binary message and emit the message event with the utf8 payload
    * */
    async HandleUTF8DataMessage(message) {
        const decodedDataMessage = Utf8DataFrame
            .Deserialize(message);
        const payload = decodedDataMessage.payload;
        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);
        this.Send(encodedAckMessage);
        this.emit("message-utf8", payload);
    }
    /*
    * Extract payload from the binary message and emit the message event with the utf8 payload
    * */
    async HandleBinaryDataMessage(message) {
        const decodedDataMessage = BinaryDataFrame
            .Deserialize(message);
        const payload = decodedDataMessage.payload;
        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);
        this.Send(encodedAckMessage);
        this.emit("message-binary", payload);
    }
    async HandleTxStartMessage(message) {
        const decodedStartFrame = TXStartFrame
            .Deserialize(message);
        const ack_id = decodedStartFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        this.Send(encodedACKMessage);
        const stream = new Readable({
            read() {
            }
        });
        //Handle stream
        stream.on("close", () => {
            this.streams.delete(decodedStartFrame.txId);
        });
        this.streams.set(decodedStartFrame.txId, stream);
        this.emit("tx-start", decodedStartFrame.txId, decodedStartFrame.txName);
    }
    async HandleTxFinishMessage(message) {
        const decodedFinishFrame = TXFinishFrame
            .Deserialize(message);
        const ack_id = decodedFinishFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        this.Send(encodedACKMessage);
        //Handle stream
        if (!this.streams.has(decodedFinishFrame.txId))
            return;
        this.streams.get(decodedFinishFrame.txId).push(null);
        this.emit("tx-finish", decodedFinishFrame.txId);
    }
    async HandleTxChunkMessage(message) {
        const decodedChunkFrame = TXChunkFrame
            .Deserialize(message);
        //Handle stream
        if (!this.streams.has(decodedChunkFrame.txId))
            return;
        this.streams.get(decodedChunkFrame.txId).push(decodedChunkFrame.payload);
        this.emit("tx-chunk", decodedChunkFrame.txId, decodedChunkFrame.payload);
    }
    async HandleByeMessage(message) {
        const decodedByeMessage = ByeFrame
            .Deserialize(message);
        const ack_id = decodedByeMessage.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        this.Send(encodedACKMessage);
        this.Destroy(4000, decodedByeMessage.reason);
    }
    async HandleEndpointInfoMessage(message) {
        const decodedInfoMessage = EndpointInfoFrame
            .Deserialize(message);
        const ack_id = decodedInfoMessage.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        this.Send(encodedACKMessage);
        //Check protocol version equality and fail otherwise
        if (CRYO_PROTOCOL_VERSION !== decodedInfoMessage.version) {
            this.Destroy(4001, `Protocol mismatch. Client offered ${decodedInfoMessage.version}, we support ${CRYO_PROTOCOL_VERSION} !`);
            return;
        }
        this.log("Got protocol features: ", this.receivedProtocolFeatures.toString(2).padStart(64));
        this.receivedProtocolFeatures = decodedInfoMessage.features;
    }
    async HandleTxFlowMessage(message) {
        const decodedFlowFrame = TXFlowFrame
            .Deserialize(message);
        const ack_id = decodedFlowFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        this.Send(encodedACKMessage);
        this.outgoingFlowControl = decodedFlowFrame.behaviour;
    }
    async HandleTxFetchMessage(message) {
        const decodedFetchFrame = TXFetchFrame
            .Deserialize(message);
        const ack_id = decodedFetchFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        this.Send(encodedACKMessage);
        this.emit("tx-fetch", decodedFetchFrame.txId, decodedFetchFrame.start, decodedFetchFrame.end);
    }
    async HandleError(err) {
        this.log(`${err.name} Exception in CryoSocket: ${err.message}`);
        this.socket.close(CryoCloseCode.CLOSE_SERVER_ERROR, `CryoSocket ${this.sid} was closed due to an error.`);
    }
    TranslateCloseCode(code) {
        switch (code) {
            case WebsocketCloseCode.NORMAL:
            case WebsocketCloseCode.GOING_AWAY:
            case CryoCloseCode.CLOSE_GRACEFUL:
                return "Connection closed normally.";
            case WebsocketCloseCode.ABNORMAL_CLOSURE:
                return "Connection closed abnormally (no close frame received).";
            case WebsocketCloseCode.INTERNAL_ERROR:
                return "Connection closed due to an internal server error.";
            case WebsocketCloseCode.SERVICE_RESTART:
                return "Connection closed because the service is restarting.";
            case WebsocketCloseCode.TRY_AGAIN_LATER:
                return "Connection closed temporarily; retry later.";
            case WebsocketCloseCode.PROTOCOL_ERROR:
                return "Connection closed due to a WebSocket protocol error.";
            case WebsocketCloseCode.UNSUPPORTED_DATA:
                return "Connection closed due to unsupported data being received.";
            case WebsocketCloseCode.INVALID_PAYLOAD:
                return "Connection closed due to invalid message payload data.";
            case WebsocketCloseCode.POLICY_VIOLATION:
                return "Connection closed due to a policy violation.";
            case WebsocketCloseCode.MESSAGE_TOO_BIG:
                return "Connection closed because a message was too large.";
            case WebsocketCloseCode.TLS_HANDSHAKE_FAILED:
                return "Connection closed due to TLS handshake failure.";
            case CryoCloseCode.CLOSE_CLIENT_ERROR:
                return "Connection closed due to a client error.";
            case CryoCloseCode.CLOSE_SERVER_ERROR:
                return "Connection closed due to a server error.";
            default:
                return "Unspecified cause for connection closure.";
        }
    }
    async HandleClose(code, reason) {
        this.log(`Websocket was closed. Code=${code} (${this.TranslateCloseCode(code)}), reason=${reason.toString("utf8")}.`);
        //Attempt to reconnect
        if (DoReconnect.includes(code)) {
            this.socket = null;
            this.socket = await this.connectionHelper.Acquire();
            this.AttachListenersToSocket(this.socket);
        }
        //Otherwise die
        if (this.socket)
            this.socket.terminate();
        this.emit("closed", code, reason.toString("utf8"));
    }
    /*
    * Send an utf8 message to the server
    * */
    SendUTF8(message) {
        const new_ack_id = this.inc_get_ack();
        const formatted_message = Utf8DataFrame
            .Serialize(this.sid, new_ack_id, message);
        this.Send(formatted_message);
    }
    /*
    * Send a binary message to the server
    * */
    SendBinary(message) {
        const new_ack_id = this.inc_get_ack();
        const formatted_message = BinaryDataFrame
            .Serialize(this.sid, new_ack_id, message);
        this.Send(formatted_message);
    }
    async StreamPush(source, streamName) {
        return new Promise((resolve, reject) => {
            const start_ack_id = this.inc_get_ack();
            const new_txid = this.inc_get_txid();
            const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName);
            this.server_ack_tracker.Track(start_ack_id, {
                message: start_frame,
                timestamp: Date.now()
            });
            this.Send(start_frame);
            let seq = 0;
            source.on("data", (chunk) => {
                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, seq++, chunk);
                this.Send(chunk_frame);
            });
            source.on("end", () => {
                const finish_ack_id = this.inc_get_ack();
                const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);
                this.server_ack_tracker.Track(finish_ack_id, {
                    message: finish_frame,
                    timestamp: Date.now()
                });
                this.Send(finish_frame);
                resolve();
            });
            source.on("error", (err) => {
                reject(err);
            });
        });
    }
    async StreamPull(source, streamName) {
        return new Promise(async (resolve, reject) => {
            let totalSize = 0;
            const chunks = [];
            source.on("data", (chunk) => {
                chunks.push(TXChunkFrame.Serialize(this.sid, new_txid, seq++, chunk));
            });
            const start_ack_id = this.inc_get_ack();
            const new_txid = this.inc_get_txid();
            const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName, totalSize);
            this.server_ack_tracker.Track(start_ack_id, {
                message: start_frame,
                timestamp: Date.now()
            });
            this.Send(start_frame);
            let seq = 0;
            const fetchHandler = async (txId, start, end) => {
                if (txId !== new_txid)
                    return;
                for (let i = start; i < end; i++) {
                    const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, seq++, chunks[i]);
                    this.Send(chunk_frame);
                }
                if (end >= chunks.length) {
                    const finish_ack_id = this.inc_get_ack();
                    const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);
                    this.server_ack_tracker.Track(finish_ack_id, {
                        message: finish_frame,
                        timestamp: Date.now()
                    });
                    this.Send(finish_frame);
                    this.removeListener("tx-fetch", fetchHandler);
                }
            };
            this.addListener("tx-fetch", fetchHandler);
        });
    }
    async Stream(source, streamName = "anonymous") {
        if (this.outgoingFlowControl !== CRYO_FLOW_BEHAVIOUR.TX_PUSH)
            return this.StreamPull(source, streamName);
        return this.StreamPush(source, streamName);
    }
    async WaitForStream(streamName = "anonymous", timeout = 1000) {
        const timeoutSig = AbortSignal.timeout(timeout);
        return new Promise((resolve, reject) => {
            const onTxStartListener = async (txId, txName) => {
                if (txName === streamName) {
                    if (!this.streams.has(txId)) {
                        this.off("tx-start", onTxStartListener);
                        timeoutSig.removeEventListener("abort", onAbort);
                        reject(new Error(`No stream id ${txId} present!`));
                    }
                    const stream = this.streams.get(txId);
                    //Remove this listener once the stream has been read
                    stream.on("close", () => {
                        this.off("tx-start", onTxStartListener);
                    });
                    resolve(stream);
                }
            };
            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            };
            this.on("tx-start", onTxStartListener);
            timeoutSig.addEventListener("abort", onAbort);
        });
    }
    //noinspection JSUnusedGlobalSymbols
    async SetIncomingFlowControl(behaviour) {
        const flow_ack_id = this.inc_get_ack();
        const flow_frame = TXFlowFrame.Serialize(this.sid, flow_ack_id, behaviour);
        this.server_ack_tracker.Track(flow_ack_id, {
            message: flow_frame,
            timestamp: Date.now()
        });
        this.Send(flow_frame);
    }
    Close() {
        this.Destroy(CryoCloseCode.CLOSE_GRACEFUL, "Client finished.");
    }
    inc_get_txid() {
        if (this.current_txid + 1 > 0xffffffff)
            this.current_txid = 0;
        return this.current_txid++;
    }
    inc_get_ack() {
        if (this.current_ack + 1 > 0xffffffff)
            this.current_ack = 0;
        return this.current_ack++;
    }
    Destroy(code = 1000, message = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }
    /**
     * Getter for the internal cryo session id
     * */
    get session_id() {
        return this.sid;
    }
    /**
     * Retrieve ewma RTT
     * */
    get rtt() {
        return this.server_ack_tracker.rtt;
    }
    /**
     * Retrieve bytes transmitted
     * */
    get tx() {
        return this.bytes_tx;
    }
    /**
     * Retrieve bytes received
     * */
    get rx() {
        return this.bytes_rx;
    }
}
