import { BufferUtil } from "../BufferUtil.js";
import { BinaryMessageType } from "../defs.js";
export class ErrorFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const ack = value.readUInt32BE(16);
        const type = value.readUint8(20);
        const payload = value.subarray(21).toString("utf8");
        if (type !== BinaryMessageType.ERROR)
            throw new Error("Attempt to deserialize a non-error message!");
        return {
            sid,
            ack,
            type,
            payload
        };
    }
    static Serialize(sid, ack, payload) {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + (payload?.length || 13));
        const sid_buf = BufferUtil.sidToBuffer(sid);
        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUInt32BE(ack, 16);
        msg_buf.writeUint8(BinaryMessageType.ERROR, 20);
        msg_buf.write(payload || "unknown_error", 21);
        return msg_buf;
    }
}
