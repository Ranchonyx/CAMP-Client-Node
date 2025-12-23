import EventEmitter from "node:events";
import { AckTracker } from "../Common/AckTracker/AckTracker.js";
import CryoFrameFormatter, { BinaryMessageType } from "../Common/CryoBinaryMessage/CryoFrameFormatter.js";
import { CryoFrameInspector } from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import { randomUUID } from "node:crypto";
import { CreateDebugLogger } from "../Common/Util/CreateDebugLogger.js";
import { CryoCryptoBox } from "./CryoCryptoBox.js";
import { CryoHandshakeEngine } from "./CryoHandshakeEngine.js";
import { CryoFrameRouter } from "./CryoFrameRouter.js";
import { CryoConnectionHelper } from "./CryoConnectionHelper.js";
var CryoCloseCode;
(function (CryoCloseCode) {
    CryoCloseCode[CryoCloseCode["CLOSE_GRACEFUL"] = 4000] = "CLOSE_GRACEFUL";
    CryoCloseCode[CryoCloseCode["CLOSE_CLIENT_ERROR"] = 4001] = "CLOSE_CLIENT_ERROR";
    CryoCloseCode[CryoCloseCode["CLOSE_SERVER_ERROR"] = 4002] = "CLOSE_SERVER_ERROR";
    CryoCloseCode[CryoCloseCode["CLOSE_CALE_MISMATCH"] = 4010] = "CLOSE_CALE_MISMATCH";
    CryoCloseCode[CryoCloseCode["CLOSE_CALE_HANDSHAKE"] = 4011] = "CLOSE_CALE_HANDSHAKE";
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
const DoNotReconnect = [WebsocketCloseCode.NORMAL, WebsocketCloseCode.GOING_AWAY, CryoCloseCode.CLOSE_GRACEFUL];
const DoReconnect = [WebsocketCloseCode.NORMAL, WebsocketCloseCode.GOING_AWAY, CryoCloseCode.CLOSE_GRACEFUL];
const Fatal = [
    WebsocketCloseCode.PROTOCOL_ERROR,
    WebsocketCloseCode.UNSUPPORTED_DATA,
    WebsocketCloseCode.ABNORMAL_CLOSURE,
    WebsocketCloseCode.INVALID_PAYLOAD,
    WebsocketCloseCode.POLICY_VIOLATION,
    WebsocketCloseCode.MESSAGE_TOO_BIG,
    WebsocketCloseCode.INTERNAL_ERROR,
    WebsocketCloseCode.SERVICE_RESTART,
    WebsocketCloseCode.TRY_AGAIN_LATER,
    WebsocketCloseCode.TLS_HANDSHAKE_FAILED,
];
/*
* Cryo Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CryoClientWebsocketSession extends EventEmitter {
    socket;
    connectionHelper;
    sid;
    use_cale;
    log;
    server_ack_tracker = new AckTracker();
    current_ack = 0;
    ping_pong_formatter = CryoFrameFormatter.GetFormatter("ping_pong");
    ack_formatter = CryoFrameFormatter.GetFormatter("ack");
    error_formatter = CryoFrameFormatter.GetFormatter("error");
    utf8_formatter = CryoFrameFormatter.GetFormatter("utf8data");
    binary_formatter = CryoFrameFormatter.GetFormatter("binarydata");
    crypto = null;
    handshake = null;
    router;
    constructor(socket, connectionHelper, sid, use_cale = true, log = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();
        this.socket = socket;
        this.connectionHelper = connectionHelper;
        this.sid = sid;
        this.use_cale = use_cale;
        this.log = log;
        if (use_cale) {
            const handshake_events = {
                onSecure: ({ transmit_key, receive_key }) => {
                    this.crypto = new CryoCryptoBox(transmit_key, receive_key);
                    this.log("Channel secured.");
                    this.emit("connected"); // only emit once we’re secure
                },
                onFailure: (reason) => {
                    this.log(`Handshake failure: ${reason}`);
                    this.Destroy(CryoCloseCode.CLOSE_CALE_HANDSHAKE, "Failure during CALE handshake.");
                }
            };
            this.handshake = new CryoHandshakeEngine(this.sid, async (buf) => this.socket.send(buf), // raw plaintext send
            CryoFrameFormatter, () => this.current_ack++, handshake_events);
            this.router = new CryoFrameRouter(CryoFrameFormatter, () => this.handshake.is_secure, (b) => this.crypto.decrypt(b), {
                on_ping_pong: async (b) => this.HandlePingPongMessage(b),
                on_ack: async (b) => this.HandleAckMessage(b),
                on_error: async (b) => this.HandleErrorMessage(b),
                on_utf8: async (b) => this.HandleUTF8DataMessage(b),
                on_binary: async (b) => this.HandleBinaryDataMessage(b),
                on_server_hello: async (b) => this.handshake.on_server_hello(b),
                on_handshake_done: async (b) => this.handshake.on_server_handshake_done(b)
            });
        }
        else {
            this.log("CALE disabled, running in unencrypted mode.");
            this.router = new CryoFrameRouter(CryoFrameFormatter, () => false, (b) => b, {
                on_ping_pong: async (b) => this.HandlePingPongMessage(b),
                on_ack: async (b) => this.HandleAckMessage(b),
                on_error: async (b) => this.HandleErrorMessage(b),
                on_utf8: async (b) => this.HandleUTF8DataMessage(b),
                on_binary: async (b) => this.HandleBinaryDataMessage(b),
                on_server_hello: async (_b) => this.Destroy(CryoCloseCode.CLOSE_CALE_MISMATCH, "CALE Mismatch. The server excepts CALE encryption, which is currently disabled.")
            });
            setImmediate(() => this.emit("connected"));
        }
        this.AttachListenersToSocket(socket);
    }
    AttachListenersToSocket(socket) {
        if (this.use_cale) {
            socket.once("message", (raw) => {
                //If the first read frame IS NOT SERVER_HELLO, fail and die in an explosion.
                const type = CryoFrameFormatter.GetType(raw);
                if (type !== BinaryMessageType.SERVER_HELLO) {
                    this.log(`CALE mismatch: expected SERVER_HELLO, got ${type}`);
                    this.Destroy(1002, "CALE mismatch: The server has disabled CALE.");
                    return;
                }
                this.router.do_route(raw).then(() => {
                    socket.on("message", async (msg) => {
                        await this.router.do_route(msg);
                    });
                });
            });
        }
        else {
            socket.on("message", async (raw) => {
                await this.router.do_route(raw);
            });
        }
        socket.on("error", this.HandleError.bind(this));
        socket.on("close", this.HandleClose.bind(this));
    }
    static async Connect(host, bearer, additionalQueryParamsMap, use_cale = true, timeout = 5000, maxPayload = 256 * 1024 * 1024) {
        const sid = randomUUID();
        const connHelper = new CryoConnectionHelper(host, bearer, sid, timeout, maxPayload, additionalQueryParamsMap);
        const socket = await connHelper.Acquire();
        return new CryoClientWebsocketSession(socket, connHelper, sid, use_cale);
    }
    /*
    * Handle an outgoing binary message
    * */
    HandleOutgoingBinaryMessage(outgoing_message) {
        //Create a pending message with a new ack number and queue it for acknowledgement by the server
        const type = CryoFrameFormatter.GetType(outgoing_message);
        if (type === BinaryMessageType.UTF8DATA || type === BinaryMessageType.BINARYDATA) {
            const message_ack = CryoFrameFormatter.GetAck(outgoing_message);
            this.server_ack_tracker.Track(message_ack, {
                timestamp: Date.now(),
                message: outgoing_message
            });
        }
        //Send the message buffer to the server
        if (!this.socket)
            return;
        let message = outgoing_message;
        if (this.use_cale && this.secure) {
            message = this.crypto.encrypt(outgoing_message);
        }
        this.socket.send(message, (maybe_error) => {
            if (maybe_error)
                this.HandleError(maybe_error).then(r => null);
        });
        this.log(`Sent ${CryoFrameInspector.Inspect(outgoing_message)} to server.`);
    }
    /*
    * Respond to PONG frames with PING and vice versa
    * */
    async HandlePingPongMessage(message) {
        const decodedPingPongMessage = this.ping_pong_formatter
            .Deserialize(message);
        const ping_pongMessage = this.ping_pong_formatter
            .Serialize(this.sid, decodedPingPongMessage.ack, decodedPingPongMessage.payload === "pong" ? "ping" : "pong");
        this.HandleOutgoingBinaryMessage(ping_pongMessage);
    }
    /*
    * Handling of binary error messages from the server, currently just log it
    * */
    async HandleErrorMessage(message) {
        const decodedErrorMessage = this.error_formatter
            .Deserialize(message);
        this.log(decodedErrorMessage.payload);
    }
    /*
    * Locally ACK the pending message if it matches the server's ACK
    * */
    async HandleAckMessage(message) {
        const decodedAckMessage = this.ack_formatter
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
        const decodedDataMessage = this.utf8_formatter
            .Deserialize(message);
        const payload = decodedDataMessage.payload;
        const encodedAckMessage = this.ack_formatter
            .Serialize(this.sid, decodedDataMessage.ack);
        this.HandleOutgoingBinaryMessage(encodedAckMessage);
        this.emit("message-utf8", payload);
    }
    /*
    * Extract payload from the binary message and emit the message event with the utf8 payload
    * */
    async HandleBinaryDataMessage(message) {
        const decodedDataMessage = this.binary_formatter
            .Deserialize(message);
        const payload = decodedDataMessage.payload;
        const encodedAckMessage = this.ack_formatter
            .Serialize(this.sid, decodedDataMessage.ack);
        this.HandleOutgoingBinaryMessage(encodedAckMessage);
        this.emit("message-binary", payload);
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
            case CryoCloseCode.CLOSE_CALE_MISMATCH:
                return "Connection closed due to a mismatch in client/server CALE configuration.";
            case CryoCloseCode.CLOSE_CALE_HANDSHAKE:
                return "Connection closed due to an error in the CALE handshake.";
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
        const new_ack_id = this.current_ack++;
        const formatted_message = CryoFrameFormatter
            .GetFormatter("utf8data")
            .Serialize(this.sid, new_ack_id, message);
        this.HandleOutgoingBinaryMessage(formatted_message);
    }
    /*
    * Send a binary message to the server
    * */
    SendBinary(message) {
        const new_ack_id = this.current_ack++;
        const formatted_message = CryoFrameFormatter
            .GetFormatter("binarydata")
            .Serialize(this.sid, new_ack_id, message);
        this.HandleOutgoingBinaryMessage(formatted_message);
    }
    Close() {
        this.Destroy(CryoCloseCode.CLOSE_GRACEFUL, "Client finished.");
    }
    get secure() {
        return this.use_cale && this.crypto !== null;
    }
    get session_id() {
        return this.sid;
    }
    Destroy(code = 1000, message = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }
}
