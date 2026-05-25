export interface ICryoClientWebsocketSessionEvents {
    "message-utf8": (message: string) => void;
    "message-binary": (message: Buffer) => void;
    "closed": (code: number, reason: string) => void;
    "connected": () => void;
    "disconnected": () => void;
    "reconnected": () => void;

    "tx-start": (txId: number, txName: string) => Promise<void>;
    "tx-chunk": (txId: number, data: Buffer) => Promise<void>;
    "tx-finish": (txId: number) => Promise<void>;
    "tx-fetch": (txId: number, start: number, end: number) => Promise<void>;
}

export type PendingBinaryMessage = {
    timestamp: number;
    message: Buffer;
    payload?: string | Buffer;
}