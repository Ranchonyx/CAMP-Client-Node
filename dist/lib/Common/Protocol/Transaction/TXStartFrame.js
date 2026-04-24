import { BinaryMessageType } from "../defs.js";
import { BufferUtil } from "../BufferUtil.js";
export class TXStartFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const ack = value.readUInt32BE(16);
        const type = value.readUint8(20);
        const txId = value.readUInt32BE(21);
        if (type !== BinaryMessageType.TX_START)
            throw new Error("Attempt to deserialize a non-tx_start message!");
        return {
            sid,
            ack,
            type,
            txId
        };
    }
    static Serialize(sid, ack, txId) {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + 4);
        const sid_buf = BufferUtil.sidToBuffer(sid);
        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUInt32BE(ack, 16);
        msg_buf.writeUint8(BinaryMessageType.TX_START, 20);
        msg_buf.writeUInt32BE(txId, 21);
        return msg_buf;
    }
}
