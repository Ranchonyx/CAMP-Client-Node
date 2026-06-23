import { ACKFrame, BufferUtil, CAMP_FEATURE_MASK_TRANSACTION, CAMP_FLOW_BEHAVIOUR, CAMPFrameType, CAMPHasFeatureFlag, TXCancelFrame, TXChunkFrame, TXFetchFrame, TXFinishFrame, TXStartFrame } from "camp-protocol";
import { Readable } from "node:stream";
import { EventEmitter } from "node:events";
import { open, stat, unlink } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { createWriteStream } from "node:fs";
import { finished } from "node:stream/promises";
import Guard from "../../Common/Util/Guard.js";
import { on } from "node:events";
export class CAMPTransactionManager extends EventEmitter {
    sid;
    send;
    next_ack;
    next_txid;
    destroy;
    get_features;
    incomingStreams = new Map();
    outgoingStreams = new Map();
    constructor(sid, send, next_ack, next_txid, destroy, get_features) {
        super();
        this.sid = sid;
        this.send = send;
        this.next_ack = next_ack;
        this.next_txid = next_txid;
        this.destroy = destroy;
        this.get_features = get_features;
    }
    /**
     * Stream a readable to the client
     * @param source The {@link Readable} object to be streamed
     * @param options The options for streaming the readable
     * */
    async Stream(source, options = { streamName: "anonymous", behaviour: "push" }) {
        const bhv = options.behaviour === "pull" ? CAMP_FLOW_BEHAVIOUR.TX_PULL : CAMP_FLOW_BEHAVIOUR.TX_PUSH;
        if (bhv === CAMP_FLOW_BEHAVIOUR.TX_PUSH)
            return this.StreamPush(source, options.streamName);
        return this.StreamPull(source, options.streamName);
    }
    /**
     * Wait for an incoming stream
     * @param streamName The name of the stream to wait for - leave empty to wait for an unnamed stream
     * @param timeout The amount of milliseconds to wait until the operation should be cancelled if no matching stream was received
     * */
    async WaitForStream(streamName = "anonymous", timeout = 2500) {
        const timeoutSig = AbortSignal.timeout(timeout);
        return new Promise((resolve, reject) => {
            const onTxStartListener = async (txId, txName) => {
                if (txName === streamName) {
                    if (!this.incomingStreams.has(txId)) {
                        this.off("tx-start", onTxStartListener);
                        timeoutSig.removeEventListener("abort", onAbort);
                        reject(new Error(`No stream id ${txId} present!`));
                    }
                    const stream = this.incomingStreams.get(txId);
                    //Remove this listener once the stream has been read
                    stream.on("close", () => {
                        this.off("tx-start", onTxStartListener);
                    });
                    resolve(stream);
                }
            };
            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            };
            this.on("tx-start", onTxStartListener);
            timeoutSig.addEventListener("abort", onAbort);
        });
    }
    /**
     * Request a range of bytes from the stream - used when flow control = TX_PULL
     * @param stream The readable object returned by {@link WaitForStream}
     * @param start The starting index of bytes to be requested
     * @param end The ending index of bytes to be requested
     * */
    async StreamRequestRange(stream, start, end) {
        const fetch_ack_id = this.next_ack();
        const fetch_frame = TXFetchFrame.Serialize(this.sid, fetch_ack_id, stream.txId, start, end);
        await this.send(fetch_frame);
    }
    /**
     * Cancels a stream
     * @param stream The readable object returned by {@link WaitForStream}
     * */
    async StreamCancel(stream) {
        const encodedCancelFrame = TXCancelFrame.Serialize(this.sid, this.next_ack(), stream.txId);
        stream.destroy();
        await this.send(encodedCancelFrame);
    }
    async StreamPush(source, streamName) {
        const new_txid = this.next_txid();
        const controller = new AbortController();
        const signal = controller.signal;
        this.outgoingStreams.set(new_txid, controller);
        try {
            //Send tx_start
            const start_ack_id = this.next_ack();
            const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName, -1n, CAMP_FLOW_BEHAVIOUR.TX_PUSH);
            await this.send(start_frame);
            //Send tx_chunk
            let offset = 0n;
            for await (const chunk of source) {
                signal.throwIfAborted();
                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, offset++, chunk);
                await this.send(chunk_frame);
                offset += BigInt(chunk.byteLength);
            }
            //send tx_finish
            const finish_ack_id = this.next_ack();
            const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);
            await this.send(finish_frame);
        }
        catch (reason) {
            //this.log(`Transaction ${new_txid} was aborted by client.`);
        }
        finally {
            this.outgoingStreams.delete(new_txid);
        }
    }
    PullTransaction = class {
        source;
        txId;
        byteLength = 0n;
        path = "";
        handle = null;
        _disposed = false;
        constructor(source, txId) {
            this.source = source;
            this.txId = txId;
        }
        /**
         * Buffers the stream once on disk in a temporary directory
         * */
        async setup() {
            this.path = path.join(tmpdir(), `CAMP-TX-${process.pid}-${this.txId}-${crypto.randomUUID()}`);
            //write source to path
            const writeStream = createWriteStream(this.path);
            this.source.pipe(writeStream, { end: true });
            await finished(writeStream);
            const stats = await stat(this.path, { bigint: true });
            this.byteLength = stats.size;
            this.handle = await open(this.path, "r");
        }
        getRangedStream(start, end) {
            Guard.AgainstNullish(this.handle);
            if (end <= start)
                return Readable.from([]);
            return this.handle.createReadStream({ start: Number(start), end: Number(end - 1n), autoClose: false });
        }
        async dispose() {
            if (this._disposed)
                return;
            Guard.AgainstNullish(this.handle);
            await this.handle.close();
            await unlink(this.path);
            this._disposed = true;
        }
    };
    StreamPull(source, streamName) {
        return new Promise(async (resolve) => {
            const start_ack_id = this.next_ack();
            const new_txid = this.next_txid();
            const controller = new AbortController();
            const signal = controller.signal;
            this.outgoingStreams.set(new_txid, controller);
            const tran = new this.PullTransaction(source, new_txid);
            await tran.setup();
            const handleFetch = async (txId, start, end) => {
                try {
                    signal.throwIfAborted();
                    const rangeStream = tran.getRangedStream(start, end);
                    let offset = start;
                    for await (const chunk of rangeStream) {
                        signal.throwIfAborted();
                        const chunk_frame = TXChunkFrame.Serialize(this.sid, txId, offset, chunk);
                        await this.send(chunk_frame);
                        offset += BigInt(chunk.byteLength);
                    }
                    if (end >= tran.byteLength) {
                        await this.send(TXFinishFrame.Serialize(this.sid, this.next_ack(), new_txid));
                        await cleanup();
                        resolve();
                    }
                }
                catch (reason) {
                    await cleanup();
                    resolve();
                }
            };
            const cleanup = async () => {
                if (signal.aborted)
                    return;
                controller.abort("cleanup");
                this.outgoingStreams.delete(new_txid);
                await tran.dispose();
            };
            try {
                const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName, tran.byteLength, CAMP_FLOW_BEHAVIOUR.TX_PULL);
                await this.send(start_frame);
                for await (const params of on(this, "tx-fetch", { signal })) {
                    const [txId, start, end] = params;
                    if (txId !== new_txid)
                        continue;
                    await handleFetch(txId, start, end);
                }
            }
            catch (reason) {
                await cleanup();
                resolve();
            }
        });
    }
    async handle(frame) {
        const type = BufferUtil.GetType(frame);
        switch (type) {
            case CAMPFrameType.TX_START:
                await this.HandleTxStart(frame);
                return;
            case CAMPFrameType.TX_CHUNK:
                await this.HandleTxChunk(frame);
                return;
            case CAMPFrameType.TX_FINISH:
                await this.HandleTxFinish(frame);
                return;
            case CAMPFrameType.TX_FETCH:
                await this.HandleTxFetch(frame);
                return;
            case CAMPFrameType.TX_CANCEL:
                await this.HandleTxCancel(frame);
        }
    }
    async HandleTxStart(frame) {
        if (this.abortIfMismatched())
            return;
        const decodedStartFrame = TXStartFrame
            .Deserialize(frame);
        const stream = new Readable({
            read() {
            }
        });
        const { txId, txName } = decodedStartFrame;
        //Handle stream
        stream.on("close", () => {
            this.incomingStreams.delete(txId);
        });
        Object.defineProperty(stream, "txId", { value: txId });
        this.incomingStreams.set(txId, stream);
        await this.acknowledge(decodedStartFrame.ack);
        this.emit("tx-start", txId, txName);
    }
    async HandleTxCancel(frame) {
        if (this.abortIfMismatched())
            return;
        const decodedCancelFrame = TXCancelFrame
            .Deserialize(frame);
        const { txId } = decodedCancelFrame;
        if (!this.outgoingStreams.has(txId))
            return;
        this.outgoingStreams.get(txId)?.abort("Cancelled by client.");
        await this.acknowledge(decodedCancelFrame.ack);
    }
    async HandleTxFinish(frame) {
        if (this.abortIfMismatched())
            return;
        const decodedFinishFrame = TXFinishFrame
            .Deserialize(frame);
        const { txId } = decodedFinishFrame;
        //Handle stream
        if (!this.incomingStreams.has(txId))
            return;
        this.incomingStreams.get(txId).push(null);
        await this.acknowledge(decodedFinishFrame.ack);
        this.emit("tx-finish", txId);
    }
    async HandleTxFetch(frame) {
        if (this.abortIfMismatched())
            return;
        const decodedFetchFrame = TXFetchFrame
            .Deserialize(frame);
        await this.acknowledge(decodedFetchFrame.ack);
        this.emit("tx-fetch", decodedFetchFrame.txId, decodedFetchFrame.start, decodedFetchFrame.end);
    }
    async HandleTxChunk(frame) {
        if (this.abortIfMismatched())
            return;
        const decodedChunkFrame = TXChunkFrame
            .Deserialize(frame);
        const { payload, txId } = decodedChunkFrame;
        if (!this.incomingStreams.has(txId))
            return;
        this.incomingStreams.get(txId).push(payload);
        this.emit("tx-chunk", txId, payload);
    }
    async acknowledge(ack_id) {
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        await this.send(encodedACKMessage);
    }
    abortIfMismatched() {
        if (!CAMPHasFeatureFlag(this.get_features(), CAMP_FEATURE_MASK_TRANSACTION)) {
            this.destroy(4002, "PROTOCOL FEATURE MISMATCH - The connected client does not support features in the namespace 'CAMP.Transaction' !");
            return true;
        }
        return false;
    }
}
