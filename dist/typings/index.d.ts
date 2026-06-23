import {EventEmitter} from "node:events";
import {Readable} from "node:stream";
import {CRYO_FLOW_BEHAVIOUR} from "cryo-protocol";
import {TXCancelFrame} from "camp-protocol";

export interface ICryoClientWebsocketSessionEvents {
    "message-utf8": (message: string) => void;
    "message-binary": (message: Buffer) => void;
    "message-error": (message: Buffer) => void;

    "closed": (code: number, reason: string) => void;
    "connected": () => void;
    "disconnected": () => void;
    "reconnected": () => void;

    "tx-start": (txId: number) => Promise<void>;
    "tx-chunk": (txId: number, data: Buffer) => Promise<void>;
    "tx-finish": (txId: number) => Promise<void>;
}


export declare class CAMPBaseManager {
    /**
     * Send a ping to the client
     * */
    public Ping(): Promise<void>;

    /**
     * Send a UTF8 message to the client
     * */
    public SendUTF8(message: string): Promise<void>;

    /**
     * Send a binary Message to the client
     * */
    public SendBinary(message: Buffer): Promise<void>;

    /**
     * Send an error message to the client
     * */
    public SendError(message: string): Promise<void>;
}

export declare class CAMPTransactionManager {
    /**
     * Stream a readable to the client
     * @param source The {@link Readable} object to be streamed
     * @param streamName Optionally, the name of the stream
     * */
    public Stream(source: Readable, options: { streamName: string, behaviour: "pull" | "push" }): Promise<void>;

    /**
     * Wait for an incoming stream
     * @param streamName The name of the stream to wait for - leave empty to wait for an unnamed stream
     * @param timeout The amount of milliseconds to wait until the operation should be cancelled if no matching stream was received
     * */
    public WaitForStream(streamName?: string, timeout?: number): Promise<CAMPReadable>;

    /**
     * Request a range of chunks from the stream - used when flow control = TX_PULL
     * @param stream The readable object returned by {@link WaitForStream}
     * @param start The starting index of chunks to be requested
     * @param end The ending index of chunks to be requested
     * */
    public StreamRequestRange(stream: CAMPReadable, start: bigint, end: bigint): Promise<void>;

    /**
     * Cancels a stream
     * @param stream The readable object returned by {@link WaitForStream}
     * */
    public async StreamCancel(stream: CAMPReadable): Promise<void>;
}

export interface CryoClientWebsocketSession {
    on<U extends keyof ICryoClientWebsocketSessionEvents>(event: U, listener: ICryoClientWebsocketSessionEvents[U]): this;

    emit<U extends keyof ICryoClientWebsocketSessionEvents>(event: U, ...args: Parameters<ICryoClientWebsocketSessionEvents[U]>): boolean;
}

export declare class CryoClientWebsocketSession extends EventEmitter implements CryoClientWebsocketSession {
    public base: CAMPBaseManager;
    public stream: CAMPTransactionManager;

    public Close(): void;

    public get rtt(): number;

    public get tx(): number;

    public get rx(): number;
}

/**
 * Create a Cryo server and attach it to an Express.js app
 * @param host - The host to connect to
 * @param bearer - The bearer token to authenticate with at the server
 * @param additionalQueryParamsMap - A record of additional parameters to be appended to the query string in the Upgrade request
 * @param timeout - How long to wait until disconnecting
 * @param maxPayloadReceived - The maximum size of receivable payloads in bytes
 **/
async function cryo(host: string, bearer: string, additionalQueryParamsMap: Record<string, string>, timeout: number = 5000, maxPayloadReceived = 256 * 1024 * 1024): Promise<CryoClientWebsocketSession>;