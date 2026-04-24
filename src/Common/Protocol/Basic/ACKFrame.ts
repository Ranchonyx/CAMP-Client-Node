import type {UUID} from "node:crypto";
import {AckMessage, BinaryMessageType} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";

export class ACKFrame {
    public static Deserialize(value: Buffer): AckMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUInt32BE(17);

        if (type !== BinaryMessageType.ACK)
            throw new Error("Attempt to deserialize a non-ack binary message!");

        return {
            sid,
            ack,
            type
        }
    }

    public static Serialize(sid: UUID, ack: number): Buffer {
        const msg_buf = Buffer.alloc(16 + 4 + 1);
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.ACK, 16);
        msg_buf.writeUInt32BE(ack, 17);

        return msg_buf;
    }
}