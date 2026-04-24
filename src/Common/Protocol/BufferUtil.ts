import type {UUID} from "node:crypto";
import {BinaryMessageType} from "./defs.js";

export class BufferUtil {
    public static sidFromBuffer(buffer: Buffer): UUID {
        const uuidv4_p1 = buffer.subarray(0, 4).toString("hex");
        const uuidv4_p2 = buffer.subarray(4, 6).toString("hex");
        const uuidv4_p3 = buffer.subarray(6, 8).toString("hex");
        const uuidv4_p4 = buffer.subarray(8, 10).toString("hex");
        const uuidv4_p5 = buffer.subarray(10, 16).toString("hex");

        return [uuidv4_p1, uuidv4_p2, uuidv4_p3, uuidv4_p4, uuidv4_p5].join("-") as UUID;
    }

    public static sidToBuffer(sid: UUID): Buffer {
        return Buffer.from(sid.replaceAll("-", ""), 'hex');
    }

    public static GetType(message: Buffer): BinaryMessageType {
        const type = message.readUint8(20);
        if (type > BinaryMessageType.TX_FINISH)
            throw new Error(`Unable to decode type from message ${message}. MAX_TYPE = 7, got ${type} !`);

        return type;
    }

    public static GetAck(message: Buffer): number {
        return message.readUint32BE(16);
    }

    public static GetSid(message: Buffer): UUID {
        return BufferUtil.sidFromBuffer(message);
    }

    public static GetPayload(message: Buffer, encoding: BufferEncoding = "utf8"): string {
        return message.subarray(21).toString(encoding);
    }

    public static Transaction = class {
        public static GetChunkTxId(message: Buffer) {
            return message.readUint32BE(17);
        }

        public static GetChunkPayload(message: Buffer, encoding: BufferEncoding = "utf8"): string {
            return message.subarray(21).toString(encoding);
        }

        public static GetTxId(message: Buffer) {
            return message.readUint32BE(21);
        }
    }
}