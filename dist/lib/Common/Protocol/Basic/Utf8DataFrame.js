import { BinaryMessageType } from "../defs.js";
import { BufferUtil } from "../BufferUtil.js";
export class Utf8DataFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const ack = value.readUInt32BE(16);
        const type = value.readUint8(20);
        const payload = value.subarray(21).toString("utf8");
        if (type !== BinaryMessageType.UTF8DATA)
            throw new Error("Attempt to deserialize a non-data utf8 message!");
        return {
            sid,
            ack,
            type,
            payload
        };
    }
    static Serialize(sid, ack, payload) {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + (payload ? Buffer.from(payload).byteLength : 4));
        const sid_buf = BufferUtil.sidToBuffer(sid);
        //Write sid to msg_buf at 0..16
        sid_buf.copy(msg_buf, 0);
        //Write ack number at 16..20
        msg_buf.writeUInt32BE(ack, 16);
        //Write message type at 20..21
        msg_buf.writeUint8(BinaryMessageType.UTF8DATA, 20);
        //Write payload at 21..len(payload)
        msg_buf.write(payload || "null", 21);
        return msg_buf;
    }
}
