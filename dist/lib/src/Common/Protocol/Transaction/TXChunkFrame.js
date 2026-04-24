import { BinaryMessageType } from "../defs.js";
import { BufferUtil } from "../BufferUtil.js";
export class TXChunkFrame {
    static Deserialize(value) {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const txId = value.readUInt32BE(17);
        const payload = value.subarray(21);
        if (type !== BinaryMessageType.TX_CHUNK)
            throw new Error("Attempt to deserialize a non-tx_chunk message!");
        return {
            sid,
            type,
            txId,
            payload
        };
    }
    static Serialize(sid, txId, payload) {
        const msg_buf = Buffer.alloc(16 + 1 + 4 + payload.byteLength);
        const sid_buf = BufferUtil.sidToBuffer(sid);
        sid_buf.copy(msg_buf, 0); //Write sid 0-16
        msg_buf.writeUint8(BinaryMessageType.TX_CHUNK, 16);
        msg_buf.writeUInt32BE(txId, 17);
        msg_buf.set(payload, 21);
        return msg_buf;
    }
}
