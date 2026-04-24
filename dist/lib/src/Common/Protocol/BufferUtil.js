import { BinaryMessageType } from "./defs.js";
export class BufferUtil {
    static sidFromBuffer(buffer) {
        const uuidv4_p1 = buffer.subarray(0, 4).toString("hex");
        const uuidv4_p2 = buffer.subarray(4, 6).toString("hex");
        const uuidv4_p3 = buffer.subarray(6, 8).toString("hex");
        const uuidv4_p4 = buffer.subarray(8, 10).toString("hex");
        const uuidv4_p5 = buffer.subarray(10, 16).toString("hex");
        return [uuidv4_p1, uuidv4_p2, uuidv4_p3, uuidv4_p4, uuidv4_p5].join("-");
    }
    static sidToBuffer(sid) {
        return Buffer.from(sid.replaceAll("-", ""), 'hex');
    }
    static GetType(message) {
        const type = message.readUint8(20);
        if (type > BinaryMessageType.TX_FINISH)
            throw new Error(`Unable to decode type from message ${message}. MAX_TYPE = 7, got ${type} !`);
        return type;
    }
    static GetAck(message) {
        return message.readUint32BE(16);
    }
    static GetSid(message) {
        return BufferUtil.sidFromBuffer(message);
    }
    static GetPayload(message, encoding = "utf8") {
        return message.subarray(21).toString(encoding);
    }
    static Transaction = class {
        static GetChunkTxId(message) {
            return message.readUint32BE(17);
        }
        static GetChunkPayload(message, encoding = "utf8") {
            return message.subarray(21).toString(encoding);
        }
        static GetTxId(message) {
            return message.readUint32BE(21);
        }
    };
}
