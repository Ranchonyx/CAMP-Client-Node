import type {UUID} from "node:crypto";
import {BinaryMessageType, TXFinishMessage} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";

export class TXFinishFrame {
    public static Deserialize(value: Buffer): TXFinishMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUInt32BE(20);

        const txId = value.readUInt32BE(21);

        if (type !== BinaryMessageType.TX_FINISH)
            throw new Error("Attempt to deserialize a non-tx_finish message!");

        return {
            sid,
            ack,
            type,
            txId
        }
    }

    public static Serialize(sid: UUID, ack: number, txId: number): Buffer {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + 4);
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.TX_FINISH, 16);
        msg_buf.writeUInt32BE(ack, 20);
        msg_buf.writeUInt32BE(txId, 24);

        return msg_buf;
    }
}
