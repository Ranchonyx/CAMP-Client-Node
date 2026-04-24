import {ICryoClientWebsocketSessionEvents, PendingBinaryMessage} from "./types/CryoClientWebsocketSession.js";
import EventEmitter from "node:events";
import {AckTracker} from "../Common/AckTracker/AckTracker.js";
import {CryoFrameInspector} from "../Common/CryoFrameInspector/CryoFrameInspector.js";
import {randomUUID, UUID} from "node:crypto";
import {DebugLoggerFunction} from "node:util";
import {CreateDebugLogger} from "../Common/Util/CreateDebugLogger.js";
import WebSocket from "ws";
import {CryoConnectionHelper} from "./CryoConnectionHelper.js";
import {BufferUtil} from "../Common/Protocol/BufferUtil.js";
import {BinaryMessageType} from "../Common/Protocol/defs.js";
import {PingPongFrame} from "../Common/Protocol/Basic/PingPongFrame.js";
import {ErrorFrame} from "../Common/Protocol/Basic/ErrorFrame.js";
import {ACKFrame} from "../Common/Protocol/Basic/ACKFrame.js";
import {Utf8DataFrame} from "../Common/Protocol/Basic/Utf8DataFrame.js";
import {TXStartFrame} from "../Common/Protocol/Transaction/TXStartFrame.js";
import {TXFinishFrame} from "../Common/Protocol/Transaction/TXFinishFrame.js";
import {TXChunkFrame} from "../Common/Protocol/Transaction/TXChunkFrame.js";
import {BinaryDataFrame} from "../Common/Protocol/Basic/BinaryDataFrame.js";
import {Readable} from "node:stream";

export interface CryoClientWebsocketSession {
    on<U extends keyof ICryoClientWebsocketSessionEvents>(event: U, listener: ICryoClientWebsocketSessionEvents[U]): this;

    emit<U extends keyof ICryoClientWebsocketSessionEvents>(event: U, ...args: Parameters<ICryoClientWebsocketSessionEvents[U]>): boolean;
}

enum CryoCloseCode {
    CLOSE_GRACEFUL = 4000,
    CLOSE_CLIENT_ERROR = 4001,
    CLOSE_SERVER_ERROR = 4002
}

enum WebsocketCloseCode {
    NORMAL = 1000,
    GOING_AWAY = 1001,
    PROTOCOL_ERROR = 1002,
    UNSUPPORTED_DATA = 1003,
    ABNORMAL_CLOSURE = 1006,
    INVALID_PAYLOAD = 1007,
    POLICY_VIOLATION = 1008,
    MESSAGE_TOO_BIG = 1009,
    INTERNAL_ERROR = 1011,
    SERVICE_RESTART = 1012,
    TRY_AGAIN_LATER = 1013,
    TLS_HANDSHAKE_FAILED = 1015
}

const DoReconnect = [WebsocketCloseCode.NORMAL, WebsocketCloseCode.GOING_AWAY, CryoCloseCode.CLOSE_GRACEFUL] as const;
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
export class CryoClientWebsocketSession extends EventEmitter implements CryoClientWebsocketSession {
    private server_ack_tracker: AckTracker = new AckTracker();
    private streams: Map<number, Readable> = new Map();

    private current_ack = 0;
    private current_txid = 0;

    private constructor(private socket: WebSocket, private connectionHelper: CryoConnectionHelper, private sid: UUID, private use_cale: boolean = true, private log: DebugLoggerFunction = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();
        this.AttachListenersToSocket(socket);
        setImmediate(() => this.emit("connected"));
    }

    private AttachListenersToSocket(socket: WebSocket) {
        socket.on("message", async (frame: Buffer) => {
            await this.routeFrame(frame);
        });

        socket.on("error", this.HandleError.bind(this));
        socket.on("close", this.HandleClose.bind(this));
    }

    public static async Connect(host: string, bearer: string, additionalQueryParamsMap: Record<string, string>, use_cale: boolean = true, timeout: number = 5000, maxPayload: number = 256 * 1024 * 1024): Promise<CryoClientWebsocketSession> {
        const sid = randomUUID();

        const connHelper = new CryoConnectionHelper(host, bearer, sid, timeout, maxPayload, additionalQueryParamsMap);

        const socket = await connHelper.Acquire();
        return new CryoClientWebsocketSession(socket, connHelper, sid, use_cale);
    }

    public async routeFrame(frame: Buffer): Promise<void> {
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
    private Send(outgoing_message: Buffer): void {
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
    private async HandlePingPongMessage(message: Buffer): Promise<void> {
        const decodedPingPongMessage = PingPongFrame
            .Deserialize(message);

        const ping_pongMessage = PingPongFrame
            .Serialize(this.sid, decodedPingPongMessage.ack, decodedPingPongMessage.payload === "pong" ? "ping" : "pong");

        this.Send(ping_pongMessage);
    }

    /*
    * Handling of binary error messages from the server, currently just log it
    * */
    private async HandleErrorMessage(message: Buffer): Promise<void> {
        const decodedErrorMessage = ErrorFrame
            .Deserialize(message);

        this.log(decodedErrorMessage.payload);
    }

    /*
    * Locally ACK the pending message if it matches the server's ACK
    * */
    private async HandleAckMessage(message: Buffer): Promise<void> {
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
    private async HandleUTF8DataMessage(message: Buffer): Promise<void> {
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
    private async HandleBinaryDataMessage(message: Buffer): Promise<void> {
        const decodedDataMessage = BinaryDataFrame
            .Deserialize(message);

        const payload = decodedDataMessage.payload;

        const encodedAckMessage = ACKFrame
            .Serialize(this.sid, decodedDataMessage.ack);

        this.Send(encodedAckMessage);
        this.emit("message-binary", payload);
    }

    private async HandleTxStartMessage(message: Buffer): Promise<void> {
        const decodedStartFrame = TXStartFrame
            .Deserialize(message);

        const ack_id = decodedStartFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.Send(encodedACKMessage);

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

    private async HandleTxFinishMessage(message: Buffer): Promise<void> {
        const decodedFinishFrame = TXFinishFrame
            .Deserialize(message);

        const ack_id = decodedFinishFrame.ack;
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.Send(encodedACKMessage);

        //Handle stream
        if (!this.streams.has(decodedFinishFrame.txId))
            return;
        this.streams.get(decodedFinishFrame.txId)!.push(null);

        this.emit("tx-finish", decodedFinishFrame.txId);
    }

    private async HandleTxChunkMessage(message: Buffer): Promise<void> {
        const decodedChunkFrame = TXChunkFrame
            .Deserialize(message);

        //Handle stream
        if (!this.streams.has(decodedChunkFrame.txId))
            return;
        this.streams.get(decodedChunkFrame.txId)!.push(decodedChunkFrame.payload);

        this.emit("tx-chunk", decodedChunkFrame.txId, decodedChunkFrame.payload);
    }

    private async HandleError(err: Error) {
        this.log(`${err.name} Exception in CryoSocket: ${err.message}`);
        this.socket.close(CryoCloseCode.CLOSE_SERVER_ERROR, `CryoSocket ${this.sid} was closed due to an error.`);
    }

    private TranslateCloseCode(code: number): string {
        switch (code as CryoCloseCode | WebsocketCloseCode) {
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

    private async HandleClose(code: number, reason: Buffer) {
        this.log(`Websocket was closed. Code=${code} (${this.TranslateCloseCode(code)}), reason=${reason.toString("utf8")}.`);

        //Attempt to reconnect
        if (DoReconnect.includes(code)) {
            (this.socket as unknown) = null;

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
    public SendUTF8(message: string): void {
        const new_ack_id = this.inc_get_ack();

        const formatted_message = Utf8DataFrame
            .Serialize(this.sid, new_ack_id, message);

        this.Send(formatted_message);
    }

    /*
    * Send a binary message to the server
    * */
    public SendBinary(message: Buffer): void {
        const new_ack_id = this.inc_get_ack();

        const formatted_message = BinaryDataFrame
            .Serialize(this.sid, new_ack_id, message);

        this.Send(formatted_message);
    }

    public async Stream(source: Readable, streamName: string = "anonymous") {
        return new Promise<void>((resolve, reject) => {
            const new_ack_id = this.inc_get_ack();
            const new_txid = this.inc_get_txid();

            const start_frame = TXStartFrame.Serialize(this.sid, new_ack_id, new_txid, streamName);
            this.Send(start_frame);

            source.on("data", (chunk: Buffer) => {
                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, chunk);
                this.Send(chunk_frame);
            });

            source.on("end", () => {
                const finish_frame = TXFinishFrame.Serialize(this.sid, this.inc_get_ack(), new_txid);
                this.Send(finish_frame);
                resolve();
            })

            source.on("error", (err) => {
                reject(err);
            })
        });
    }

    public async WaitForStream(streamName: string = "anonymous", timeout: number = 1000): Promise<Readable> {
        const timeoutSig = AbortSignal.timeout(timeout);

        return new Promise<Readable>((resolve, reject) => {
            const onTxStartListener = async (txId: number, txName: string) => {
                if (txName === streamName) {
                    if (!this.streams.has(txId)) {
                        this.off("tx-start", onTxStartListener);
                        timeoutSig.removeEventListener("abort", onAbort);

                        reject(new Error(`No stream id ${txId} present!`));
                    }

                    const stream = this.streams.get(txId)!;

                    //Remove this listener once the stream has been read
                    stream.on("close", () => {
                        this.off("tx-start", onTxStartListener);
                    })

                    resolve(stream);
                }
            }

            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            }

            this.on("tx-start", onTxStartListener);
            timeoutSig.addEventListener("abort", onAbort);
        });
    }

    public Close(): void {
        this.Destroy(CryoCloseCode.CLOSE_GRACEFUL, "Client finished.");
    }

    public get session_id(): UUID {
        return this.sid;
    }

    private inc_get_txid(): number {
        if (this.current_txid + 1 > 0xffffffff)
            this.current_txid = 0;

        return this.current_txid++;
    }

    private inc_get_ack(): number {
        if (this.current_ack + 1 > 0xffffffff)
            this.current_ack = 0;

        return this.current_ack++;
    }

    public Destroy(code: number = 1000, message: string = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }
}