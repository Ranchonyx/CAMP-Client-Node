import EventEmitter from "node:events";
import {AckTracker} from "../Common/AckTracker/AckTracker.js";
import {DebugLoggerFunction} from "node:util";
import {CreateDebugLogger} from "../Common/Util/CreateDebugLogger.js";
import WebSocket from "ws";
import {CAMPConnectionHelper} from "./CAMPConnectionHelper.js";
import {CAMPBaseManager} from "./Namespaces/CAMP.Base.js";
import {CAMPTransactionManager} from "./Namespaces/CAMP.Transaction.js";
import {BufferUtil, CAMPFrameType, CAMPNewId, EndpointInfoFrame} from "camp-protocol";
import {CAMPFrameInspector} from "../Common/CAMPFrameInspector/CAMPFrameInspector.js";

export interface ICAMPClientWebsocketSessionEvents {
    "message-utf8": (message: string) => void;
    "message-binary": (message: Buffer) => void;
    "message-error": (message: string) => void;

    "closed": (code: number, reason: string) => void;
    "connected": () => void;
    "disconnected": () => void;
    "reconnected": () => void;
}

export interface CAMPClientWebsocketSession {
    on<U extends keyof ICAMPClientWebsocketSessionEvents>(event: U, listener: ICAMPClientWebsocketSessionEvents[U]): this;

    emit<U extends keyof ICAMPClientWebsocketSessionEvents>(event: U, ...args: Parameters<ICAMPClientWebsocketSessionEvents[U]>): boolean;
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
* Cryo Websocket session layer. Handles Binary formatting and ACKs and whatnot
* */
export class CAMPClientWebsocketSession extends EventEmitter implements CAMPClientWebsocketSession {
    private server_ack_tracker: AckTracker = new AckTracker();

    private bytes_rx: number = 0;
    private bytes_tx: number = 0;

    private current_ack = 0;
    private current_txid = 0;

    private receivedProtocolFeatures: bigint = 0n;

    private base: CAMPBaseManager;
    private stream: CAMPTransactionManager | null = null;

    private bind<T extends Function>(func: T): T {
        return func.bind(this);
    }

    private forwardMessageStrOrBuf(source: EventEmitter, event: keyof ICAMPClientWebsocketSessionEvents) {
        source.on(event, (message) => this.emit(event, message));
    }

    private constructor(private socket: WebSocket, private connectionHelper: CAMPConnectionHelper, private sid: bigint, private log: DebugLoggerFunction = CreateDebugLogger("CRYO_CLIENT_SESSION")) {
        super();

        this.base = new CAMPBaseManager(
            this.sid,
            this.bind(this.send),
            this.bind(this.next_ack),
            this.bind(this.Destroy),
            (features) => this.receivedProtocolFeatures = features,
            this.server_ack_tracker
        );

        this.forwardMessageStrOrBuf(this.base, "message-binary");
        this.forwardMessageStrOrBuf(this.base, "message-utf8");
        this.forwardMessageStrOrBuf(this.base, "message-error");

        this.AttachListenersToSocket(socket);

        this.base.on("ready", () => {
            this.stream = new CAMPTransactionManager(
                this.sid,
                this.bind(this.send),
                this.bind(this.next_ack),
                this.bind(this.next_txid),
                this.bind(this.Destroy),
                () => this.receivedProtocolFeatures
            );
            this.emit("connected");
        })

        //Send the first endpointInfo message
        const msg = EndpointInfoFrame.Serialize(this.sid, this.next_ack());
        this.send(msg);
    }

    private AttachListenersToSocket(socket: WebSocket) {
        socket.on("message", async (raw: Buffer) => {
            this.bytes_rx += raw.byteLength;
            await this.routeFrame(raw);
        });

        socket.on("error", this.HandleError.bind(this));
        socket.on("close", this.HandleClose.bind(this));
    }

    public static async Connect(host: string, bearer: string, additionalQueryParamsMap: Record<string, string>, timeout: number = 5000, maxPayload: number = 256 * 1024 * 1024): Promise<CAMPClientWebsocketSession> {
        const sid = CAMPNewId();

        const connHelper = new CAMPConnectionHelper(host, bearer, sid, timeout, maxPayload, additionalQueryParamsMap);

        const socket = await connHelper.Acquire();
        return new CAMPClientWebsocketSession(socket, connHelper, sid);
    }

    private async routeFrame(frame: Buffer) {
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
    private async send(outgoing_message: Buffer): Promise<void> {
        let ackPromise: PromiseWithResolvers<void> | null = null;

        if (!this.socket)
            return Promise.reject("No socket.");

        if (this.socket.readyState === WebSocket.CLOSING || this.socket.readyState === WebSocket.CLOSED)
            return Promise.reject("Invalid socket state.");

        const type = BufferUtil.GetType(outgoing_message);
        if (
            type === CAMPFrameType.UTF8DATA ||
            type === CAMPFrameType.BINARYDATA ||
            type === CAMPFrameType.ERROR ||
            type === CAMPFrameType.ENDPOINT_INFO ||
            type === CAMPFrameType.TX_START ||
            type === CAMPFrameType.TX_FINISH ||
            type === CAMPFrameType.TX_FETCH
        ) {
            const message_ack = BufferUtil.GetAck(outgoing_message);
            ackPromise = Promise.withResolvers<void>();
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

    public Close(): void {
        this.Destroy(CryoCloseCode.CLOSE_GRACEFUL, "Client finished.");
    }

    private next_txid(): number {
        if (this.current_txid + 1 > 0xffffffff)
            this.current_txid = 0;

        return this.current_txid++;
    }

    private next_ack(): number {
        if (this.current_ack + 1 > 0xffffffff)
            this.current_ack = 0;

        return this.current_ack++;
    }

    private Destroy(code: number = 1000, message: string = "") {
        this.log(`Teardown of session. Code=${code}, reason=${message}`);
        this.socket.close(code, message);
    }

    /**
     * Retrieve ewma RTT
     * */
    public get rtt() {
        return this.server_ack_tracker.rtt;
    }

    /**
     * Retrieve bytes transmitted
     * */
    public get tx() {
        return this.bytes_tx;
    }

    /**
     * Retrieve bytes received
     * */
    public get rx() {
        return this.bytes_rx;
    }
}