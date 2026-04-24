import type {UUID} from "node:crypto";
import {BufferUtil} from "../BufferUtil.js";
import {BinaryMessageType, ErrorMessage} from "../defs.js";

export class ErrorFrame {
    public static Deserialize(value: Buffer): ErrorMessage {
        const sid = BufferUtil.sidFromBuffer(value);
        const type = value.readUint8(16);
        const ack = value.readUInt32BE(17);
        const payload = value.subarray(21).toString("utf8") as ErrorMessage["payload"];

        if (type !== BinaryMessageType.ERROR)
            throw new Error("Attempt to deserialize a non-error message!");

        return {
            sid,
            ack,
            type,
            payload
        }
    }

    public static Serialize(sid: UUID, ack: number, payload: ErrorMessage["payload"] | null): Buffer {
        const msg_buf = Buffer.alloc(16 + 4 + 1 + (payload?.length || 13));
        const sid_buf = BufferUtil.sidToBuffer(sid);

        sid_buf.copy(msg_buf, 0);
        msg_buf.writeUint8(BinaryMessageType.ERROR, 16);
        msg_buf.writeUInt32BE(ack, 20);

        msg_buf.write(payload || "unknown_error", 21);

        return msg_buf;
    }
}