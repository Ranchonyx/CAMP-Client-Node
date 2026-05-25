import {BinaryMessageType, BufferUtil} from "cryo-protocol";

const typeToStringMap: Record<BinaryMessageType, string> = {
    255: "endpoint_info",
    254: "bye",
    253: "ack",
    252: "error",
    251: "ping/pong",
    250: "utf8data",
    249: "binarydata",
    0: "transaction_start",
    1: "transaction_chunk",
    2: "transaction_finish",
    3: "transaction_flow",
    4: "transaction_chunk_request",
}

export class CryoFrameInspector {
    public static Inspect(message: Buffer): string {
        const sid = BufferUtil.GetSid(message);
        const type = BufferUtil.GetType(message);
        const type_str = typeToStringMap[type] || "unknown";
        const ack = BufferUtil.GetAck(message);

        //For Cryo.Transaction
        if (type >= BinaryMessageType.TX_START && type <= BinaryMessageType.TX_FLOW) {
            switch (type) {
                case BinaryMessageType.TX_START:
                    return `[type=${type_str}, sid=${sid},ack=${ack},txid=${BufferUtil.Transaction.GetTxId(message)},name=${BufferUtil.Transaction.GetTxName(message)}]`;
                case BinaryMessageType.TX_FINISH:
                    return `[type=${type_str}, sid=${sid},ack=${ack},txid=${BufferUtil.Transaction.GetTxId(message)}]`;
                case BinaryMessageType.TX_CHUNK:
                    return `[type=${type_str}, sid=${sid},txid=${BufferUtil.Transaction.GetChunkTxId(message)},payload[0..15]=${BufferUtil.Transaction.GetChunkPayload(message, "hex").substring(0, 0xf)}]`;
                case BinaryMessageType.TX_FLOW:
                    return `[type=${type_str}, sid=${sid},ack=${ack},behaviour=${message.readUint8(10) === 0 ? "PUSH" : "PULL"}]`;
            }
            throw new Error("Unknown type " + type);
        } else {
            const payload = BufferUtil.GetPayload(message, "hex").substring(0, 0xf);
            return `[type=${type_str}, sid=${sid},ack=${ack},payload[0..15]=${payload}]`;
        }
    }
}