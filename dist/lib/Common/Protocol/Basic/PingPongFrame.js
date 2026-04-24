import { BinaryMessageType } from "../defs.js";
import { BufferUtil } from "../BufferUtil.js";
export class PingPongFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUInt32BE(17);
        const payload = value.subarray(21).toString("utf8");
        if (type !== BinaryMessageType.PING_PONG)
            throw new Error("Attempt to deserialize a non-ping_pong binary message!");
        if (!(payload === "ping" || payload === "pong"))
            throw new Error(`Invalid payload ${payload} in ping_pong binary message!`);
        return {
            sid,
            ack,
            type,
            payload
        };
    }
    static Serialize(sid, ack, payload) {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + 4);
        const sid_buf = BufferUtil.sidToBuffer(sid);
        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.PING_PONG, 16);
        msg_buf.writeUInt32BE(ack, 17);
        msg_buf.write(payload, 21);
        return msg_buf;
    }
}
