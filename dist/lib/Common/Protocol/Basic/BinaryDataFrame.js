import { BinaryMessageType } from "../defs.js";
import { BufferUtil } from "../BufferUtil.js";
export class BinaryDataFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const ack = value.readUInt32BE(16);
        const type = value.readUint8(20);
        const payload = value.subarray(21);
        if (type !== BinaryMessageType.BINARYDATA)
            throw new Error("Attempt to deserialize a non-data binary message!");
        return {
            sid,
            ack,
            type,
            payload
        };
    }
    static Serialize(sid, ack, payload) {
        const payload_length = payload ? payload.byteLength : 4;
        const msg_buf = Buffer.alloc(16 + 4 + 1 + payload_length);
        const sid_buf = BufferUtil.sidToBuffer(sid);
        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUInt32BE(ack, 16);
        msg_buf.writeUint8(BinaryMessageType.BINARYDATA, 20);
        msg_buf.set(payload || Buffer.from("null", "utf-8"), 21);
        return msg_buf;
    }
}
