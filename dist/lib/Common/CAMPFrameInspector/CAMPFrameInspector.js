import { CAMPFrameType, BufferUtil } from "camp-protocol";
const typeToStringMap = {
    255: "endpoint_info",
    254: "bye",
    253: "ack",
    252: "error",
    251: "ping_pong",
    250: "utf8data",
    249: "binarydata",
    0: "tx_start",
    1: "tx_chunk",
    2: "tx_finish",
    3: "tx_fetch",
    4: "tx_cancel",
};
const behaviourToStringMap = {
    0: "tx_push",
    1: "tx_pull",
};
export class CAMPFrameInspector {
    static Inspect(message) {
        const sid = BufferUtil.GetSid(message);
        const type = BufferUtil.GetType(message);
        const typeStr = typeToStringMap[type] ?? `unknown(${type})`;
        switch (type) {
            case CAMPFrameType.ENDPOINT_INFO:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `ver=${BufferUtil.EndpointInfo.GetVersion(message)}`,
                    `flags=${BufferUtil.EndpointInfo.GetFlags(message)}]`,
                ].join(",");
            case CAMPFrameType.BYE:
                return `[type=${typeStr},sid=${sid},ack=${BufferUtil.GetAck(message)}]`;
            case CAMPFrameType.ACK:
                return `[type=${typeStr},sid=${sid},ackedAck=${BufferUtil.GetAck(message)}]`;
            case CAMPFrameType.ERROR:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `payload="${BufferUtil.GetPayload(message, "utf8").substring(0, 64)}"]`,
                ].join(",");
            case CAMPFrameType.PING_PONG:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `payload="${BufferUtil.GetPayload(message, "utf8")}"]`,
                ].join(",");
            case CAMPFrameType.UTF8DATA:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `payload="${BufferUtil.GetPayload(message, "utf8").substring(0, 64)}"]`,
                ].join(",");
            case CAMPFrameType.BINARYDATA:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `payload[0..16]=${BufferUtil.GetPayload(message, "hex").substring(0, 32)}]`,
                ].join(",");
            case CAMPFrameType.TX_START: {
                const behaviour = BufferUtil.Transaction.GetFlowBehaviour(message);
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `txid=${BufferUtil.Transaction.GetTxId(message)}`,
                    `size=${BufferUtil.Transaction.GetTxSize(message)}`,
                    `behaviour=${behaviourToStringMap[behaviour] ?? `unknown(${behaviour})`}`,
                    `name="${BufferUtil.Transaction.GetTxName(message)}"]`,
                ].join(",");
            }
            case CAMPFrameType.TX_CHUNK:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `txid=${BufferUtil.Transaction.GetChunkTxId(message)}`,
                    `offset=${BufferUtil.Transaction.GetChunkOffset(message)}`,
                    `payload[0..16]=${BufferUtil.Transaction.GetChunkPayload(message, "hex").substring(0, 32)}]`,
                ].join(",");
            case CAMPFrameType.TX_FINISH:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `txid=${BufferUtil.Transaction.GetTxId(message)}]`,
                ].join(",");
            case CAMPFrameType.TX_FETCH:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `txid=${BufferUtil.Transaction.GetFetchTxId(message)}`,
                    `start=${BufferUtil.Transaction.GetFetchStart(message)}`,
                    `end=${BufferUtil.Transaction.GetFetchEnd(message)}]`,
                ].join(",");
            case CAMPFrameType.TX_CANCEL:
                return [
                    `[type=${typeStr}`,
                    `sid=${sid}`,
                    `ack=${BufferUtil.GetAck(message)}`,
                    `txid=${BufferUtil.Transaction.GetTxId(message)}]`,
                ].join(",");
            default:
                return `[type=${typeStr},sid=${sid},payload[0..16]=${BufferUtil.GetPayload(message, "hex").substring(0, 32)}]`;
        }
    }
}
