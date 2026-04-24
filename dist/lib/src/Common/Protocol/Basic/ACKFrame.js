import { BinaryMessageType } from "../defs.js";
import { BufferUtil } from "../BufferUtil.js";
export class ACKFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const ack = value.readUInt32BE(16);
        const type = value.readUint8(20);
        if (type !== BinaryMessageType.ACK)
            throw new Error("Attempt to deserialize a non-ack binary message!");
        return {
            sid,
            ack,
            type
        };
    }
    static Serialize(sid, ack) {
        const msg_buf = Buffer.alloc(16 + 4 + 1);
        const sid_buf = BufferUtil.sidToBuffer(sid);
        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUInt32BE(ack, 16);
        msg_buf.writeUint8(BinaryMessageType.ACK, 20);
        return msg_buf;
    }
}
