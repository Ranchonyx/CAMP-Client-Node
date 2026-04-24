import {EventEmitter} from "node:events";
import {Readable} from "node:stream";

export interface ICryoClientWebsocketSessionEvents {
    "message-utf8": (message: string) => void;
    "message-binary": (message: Buffer) => void;
    "closed": (code: number, reason: string) => void;
    "connected": () => void;
    "disconnected": () => void;
    "reconnected": () => void;

    "tx-start": (txId: number) => Promise<void>;
    "tx-chunk": (txId: number, data: Buffer) => Promise<void>;
    "tx-finish": (txId: number) => Promise<void>;
}

export interface CryoClientWebsocketSession {
    on<U extends keyof ICryoClientWebsocketSessionEvents>(event: U, listener: ICryoClientWebsocketSessionEvents[U]): this;

    emit<U extends keyof ICryoClientWebsocketSessionEvents>(event: U, ...args: Parameters<ICryoClientWebsocketSessionEvents[U]>): boolean;
}

export declare class CryoClientWebsocketSession extends EventEmitter implements CryoClientWebsocketSession {
    public SendUTF8(message: string): void;

    public SendBinary(message: Buffer): void;

    public Stream(source: Readable): Promise<void>;

    public Close(): void;
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