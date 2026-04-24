import type {UUID} from "node:crypto";
import {BinaryMessageType, TXStartMessage} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";

export class TXStartFrame {
    public static Deserialize(value: Buffer): TXStartMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUInt32BE(17);
        const txId = value.readUInt32BE(21);
        const txName = value.subarray(22).toString("utf8");

        if (type !== BinaryMessageType.TX_START)
            throw new Error("Attempt to deserialize a non-tx_start message!");

        return {
            sid,
            ack,
            type,
            txId,
            txName
        }
    }

    public static Serialize(sid: UUID, ack: number, txId: number, name: string): Buffer {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + 4);
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.TX_START, 16);
        msg_buf.writeUInt32BE(ack, 17);
        msg_buf.writeUInt32BE(txId, 21);
        msg_buf.set(Buffer.from(name, "utf8"), 22)

        return msg_buf;
    }
}
