import type {UUID} from "node:crypto";

export enum BinaryMessageType {
    ACK = 0,
    ERROR = 1,
    PING_PONG = 2,
    UTF8DATA = 3,
    BINARYDATA = 4,
    TX_START = 5,
    TX_CHUNK = 6,
    TX_FINISH = 7
}

export type BinaryMessage<T, U extends BinaryMessageType> = {
    sid: UUID;
    type: U;
} & T;

export type AckMessage = BinaryMessage<{
    ack: number;
}, BinaryMessageType.ACK>;

export type PingMessage = BinaryMessage<{
    ack: number;
    payload: "ping" | "pong";
}, BinaryMessageType.PING_PONG>;

export type UTF8DataMessage = BinaryMessage<{
    ack: number;
    payload: string;
}, BinaryMessageType.UTF8DATA>;

export type BinaryDataMessage = BinaryMessage<{
    ack: number;
    payload: Buffer;
}, BinaryMessageType.BINARYDATA>;

export type TXStartMessage = BinaryMessage<{
    ack: number;
    txId: number;
}, BinaryMessageType.TX_START>;

export type TXChunkMessage = BinaryMessage<{
    payload: Buffer;
    txId: number;
}, BinaryMessageType.TX_CHUNK>;

export type TXFinishMessage = BinaryMessage<{
    ack: number;
    txId: number;
}, BinaryMessageType.TX_FINISH>;

export type ErrorMessage = BinaryMessage<{
    ack: number;
    payload: "invalid_operation" | "session_expired" | "error";
}, BinaryMessageType.ERROR>;