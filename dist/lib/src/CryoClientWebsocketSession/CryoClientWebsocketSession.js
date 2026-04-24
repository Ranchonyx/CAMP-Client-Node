import EventEmitter from "node:events";
import { AckTracker } from "../Common/AckTracker/AckTracker.js";
import { CryoFrameInspector } from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import { randomUUID } from "node:crypto";
import { CreateDebugLogger } from "../Common/Util/CreateDebugLogger.js";
import { CryoConnectionHelper } from "./CryoConnectionHelper.js";
import { BufferUtil } from "../Common/Protocol/BufferUtil.js";
import { BinaryMessageType } from "../Common/Protocol/defs.js";
import { PingPongFrame } from "../Common/Protocol/Basic/PingPongFrame.js";
import { ErrorFrame } from "../Common/Protocol/Basic/ErrorFrame.js";
import { ACKFrame } from "../Common/Protocol/Basic/ACKFrame.js";
import { Utf8DataFrame } from "../Common/Protocol/Basic/Utf8DataFrame.js";
import { TXStartFrame } from "../Common/Protocol/Transaction/TXStartFrame.js";
import { TXFinishFrame } from "../Common/Protocol/Transaction/TXFinishFrame.js";
import { TXChunkFrame } from "../Common/Protocol/Transaction/TXChunkFrame.js";
import { BinaryDataFrame } from "../Common/Protocol/Basic/BinaryDataFrame.js";
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
const DoNotReconnect = [WebsocketCloseCode.NORMAL, WebsocketCloseCode.GOING_AWAY, CryoCloseCode.CLOSE_GRACEFUL] as const;
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
*/
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
    current_txid = 0;
    constructor(socket, connectionHelper, sid, use_cale = true, log = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();
        this.socket = socket;
        this.connectionHelper = connectionHelper;
        this.sid = sid;
        this.use_cale = use_cale;
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
    static async Connect(host, bearer, additionalQueryParamsMap, use_cale = true, timeout = 5000, maxPayload = 256 * 1024 * 1024) {
        const sid = randomUUID();
        const connHelper = new CryoConnectionHelper(host, bearer, sid, timeout, maxPayload, additionalQueryParamsMap);
        const socket = await connHelper.Acquire();
        return new CryoClientWebsocketSession(socket, connHelper, sid, use_cale);
    }
    async routeFrame(frame) {
        const type = BufferUtil.GetType(frame);
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
        await this.Send(encodedACKMessage);
        this.emit("tx-start", decodedStartFrame.txId);
    }
    async HandleTxFinishMessage(message) {
        const decodedFinishFrame = TXFinishFrame
            .Deserialize(message);
        const ack_id = decodedFinishFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        await this.Send(encodedACKMessage);
        this.emit("tx-finish", decodedFinishFrame.txId);
    }
    async HandleTxChunkMessage(message) {
        const decodedChunkFrame = TXChunkFrame
            .Deserialize(message);
        this.emit("tx-chunk", decodedChunkFrame.txId, decodedChunkFrame.payload);
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
    async Stream(source) {
        return new Promise((resolve, reject) => {
            const new_ack_id = this.inc_get_ack();
            const new_txid = this.inc_get_txid();
            const start_frame = TXStartFrame.Serialize(this.sid, new_ack_id, new_txid);
            this.Send(start_frame);
            source.on("data", (chunk) => {
                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, chunk);
                this.Send(chunk_frame);
            });
            source.on("end", () => {
                const finish_frame = TXFinishFrame.Serialize(this.sid, this.inc_get_ack(), new_txid);
                this.Send(finish_frame);
                resolve();
            });
            source.on("error", (err) => {
                reject(err);
            });
        });
    }
    Close() {
        this.Destroy(CryoCloseCode.CLOSE_GRACEFUL, "Client finished.");
    }
    get session_id() {
        return this.sid;
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
}
