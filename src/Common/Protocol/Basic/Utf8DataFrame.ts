import type {UUID} from "node:crypto";
import {BinaryMessageType, UTF8DataMessage} from "../defs.js";
import {BufferUtil} from "../BufferUtil.js";

export class Utf8DataFrame {
    public static Deserialize(value: Buffer): UTF8DataMessage {
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
        }
    }

    public static Serialize(sid: UUID, ack: number, payload: string | null): Buffer {
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