import EventEmitter from "node:events";
import { AckTracker } from "../Common/AckTracker/AckTracker.js";
import { CreateDebugLogger } from "../Common/Util/CreateDebugLogger.js";
import WebSocket from "ws";
import { CAMPConnectionHelper } from "./CAMPConnectionHelper.js";
import { CAMPBaseManager } from "./Namespaces/CAMP.Base.js";
import { CAMPTransactionManager } from "./Namespaces/CAMP.Transaction.js";
import { BufferUtil, CAMPFrameType, CAMPNewId, EndpointInfoFrame } from "camp-protocol";
import { CAMPFrameInspector } from "../Common/CAMPFrameInspector/CAMPFrameInspector.js";
var CloseCode;
(function (CloseCode) {
    CloseCode[CloseCode["CLOSE_GRACEFUL"] = 4000] = "CLOSE_GRACEFUL";
    CloseCode[CloseCode["CLOSE_CLIENT_ERROR"] = 4001] = "CLOSE_CLIENT_ERROR";
    CloseCode[CloseCode["CLOSE_SERVER_ERROR"] = 4002] = "CLOSE_SERVER_ERROR";
})(CloseCode || (CloseCode = {}));
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
const DoReconnect = [WebsocketCloseCode.NORMAL, WebsocketCloseCode.GOING_AWAY, CloseCode.CLOSE_GRACEFUL];
/*
* CAMP Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CAMPClientWebsocketSession extends EventEmitter {
    socket;
    connectionHelper;
    sid;
    log;
    server_ack_tracker = new AckTracker();
    bytes_rx = 0;
    bytes_tx = 0;
    current_ack = 0;
    current_txid = 0;
    receivedProtocolFeatures = 0n;
    base;
    stream = null;
    bind(func) {
        return func.bind(this);
    }
    forwardMessageStrOrBuf(source, event) {
        source.on(event, (message) => this.emit(event, message));
    }
    constructor(socket, connectionHelper, sid, log = CreateDebugLogger("CAMP_CLIENT_SESSION")) {
        super();
        this.socket = socket;
        this.connectionHelper = connectionHelper;
        this.sid = sid;
        this.log = log;
        this.base = new CAMPBaseManager(this.sid, this.bind(this.send), this.bind(this.next_ack), this.bind(this.Destroy), (features) => this.receivedProtocolFeatures = features, this.server_ack_tracker);
        this.forwardMessageStrOrBuf(this.base, "message-binary");
        this.forwardMessageStrOrBuf(this.base, "message-utf8");
        this.forwardMessageStrOrBuf(this.base, "message-error");
        this.AttachListenersToSocket(socket);
        this.base.on("ready", () => {
            this.stream = new CAMPTransactionManager(this.sid, this.bind(this.send), this.bind(this.next_ack), this.bind(this.next_txid), this.bind(this.Destroy), () => this.receivedProtocolFeatures);
            this.emit("connected");
        });
        //Send the first endpointInfo message
        const msg = EndpointInfoFrame.Serialize(this.sid, this.next_ack());
        this.send(msg);
    }
    AttachListenersToSocket(socket) {
        socket.on("message", async (raw) => {
            this.bytes_rx += raw.byteLength;
            await this.routeFrame(raw);
        });
        socket.on("error", this.HandleError.bind(this));
        socket.on("close", this.HandleClose.bind(this));
    }
    static async Connect(host, bearer, additionalQueryParamsMap, timeout = 5000, maxPayload = 256 * 1024 * 1024) {
        const sid = CAMPNewId();
        const connHelper = new CAMPConnectionHelper(host, bearer, sid, timeout, maxPayload, additionalQueryParamsMap);
        const socket = await connHelper.Acquire();
        return new CAMPClientWebsocketSession(socket, connHelper, sid);
    }
    async routeFrame(frame) {
        const type = BufferUtil.GetType(frame);
        if (type >= CAMPFrameType.BINARYDATA && type <= CAMPFrameType.ENDPOINT_INFO)
            return this.base.handle(frame);
        if (type >= CAMPFrameType.TX_START && type <= CAMPFrameType.TX_CANCEL)
            return this.stream?.handle(frame);
        throw new Error(`Unknown frame type ${type}!`);
    }
    /*
    * Handle an outgoing binary message
    * */
    async send(outgoing_message) {
        let ackPromise = null;
        if (!this.socket)
            return Promise.reject("No socket.");
        if (this.socket.readyState === WebSocket.CLOSING || this.socket.readyState === WebSocket.CLOSED)
            return Promise.reject("Invalid socket state.");
        const type = BufferUtil.GetType(outgoing_message);
        if (type === CAMPFrameType.UTF8DATA ||
            type === CAMPFrameType.BINARYDATA ||
            type === CAMPFrameType.ERROR ||
            type === CAMPFrameType.ENDPOINT_INFO ||
            type === CAMPFrameType.TX_START ||
            type === CAMPFrameType.TX_FINISH ||
            type === CAMPFrameType.TX_FETCH) {
            const message_ack = BufferUtil.GetAck(outgoing_message);
            ackPromise = Promise.withResolvers();
            this.server_ack_tracker.Track(message_ack, {
                timestamp: Date.now(),
                message: outgoing_message,
                ackPromise
            });
        }
        this.socket.send(outgoing_message, (maybe_error) => {
            if (maybe_error)
                this.HandleError(maybe_error).then(r => null);
            else
                this.bytes_tx += outgoing_message.byteLength;
        });
        this.log(`Sent ${CAMPFrameInspector.Inspect(outgoing_message)} to server.`);
        if (!ackPromise)
            return Promise.resolve();
        return ackPromise.promise;
    }
    async HandleError(err) {
        this.log(`${err.name} Exception in CAMPSocket: ${err.message}`);
        this.socket.close(CloseCode.CLOSE_SERVER_ERROR, `CAMPSocket ${this.sid} was closed due to an error.`);
    }
    TranslateCloseCode(code) {
        switch (code) {
            case WebsocketCloseCode.NORMAL:
            case WebsocketCloseCode.GOING_AWAY:
            case CloseCode.CLOSE_GRACEFUL:
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
            case CloseCode.CLOSE_CLIENT_ERROR:
                return "Connection closed due to a client error.";
            case CloseCode.CLOSE_SERVER_ERROR:
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
    Close() {
        this.Destroy(CloseCode.CLOSE_GRACEFUL, "Client finished.");
    }
    next_txid() {
        if (this.current_txid + 1 > 0xffffffff)
            this.current_txid = 0;
        return this.current_txid++;
    }
    next_ack() {
        if (this.current_ack + 1 > 0xffffffff)
            this.current_ack = 0;
        return this.current_ack++;
    }
    Destroy(code = 1000, message = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
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
