import {
    ACKFrame,
    BufferUtil,
    CAMP_FEATURE_MASK_TRANSACTION,
    CAMP_FLOW_BEHAVIOUR,
    CAMPFrameType,
    CAMPHasFeatureFlag,
    TXCancelFrame,
    TXChunkFrame,
    TXFetchFrame,
    TXFinishFrame,
    TXStartFrame
} from "camp-protocol";
import {Readable} from "node:stream";
import {EventEmitter} from "node:events";
import {FileHandle, open, stat, unlink} from "node:fs/promises";
import path from "node:path";
import {tmpdir} from "node:os";
import {createWriteStream} from "node:fs";
import {finished} from "node:stream/promises";
import Guard from "../../Common/Util/Guard.js";
import {on} from "node:events"

type CAMPReadable = Readable & { txId: number };

interface CAMPTransactionManagerEvents {
    "tx-start": (txId: number, txName: string) => Promise<void>;
    "tx-chunk": (txId: number, data: Buffer) => Promise<void>;
    "tx-finish": (txId: number) => Promise<void>;
    "tx-fetch": (txId: number, start: bigint, end: bigint) => Promise<void>;
}

export interface CAMPTransactionManager {
    on<U extends keyof CAMPTransactionManagerEvents>(event: U, listener: CAMPTransactionManagerEvents[U]): this;

    emit<U extends keyof CAMPTransactionManagerEvents>(event: U, ...args: Parameters<CAMPTransactionManagerEvents[U]>): boolean;
}

export class CAMPTransactionManager extends EventEmitter implements CAMPTransactionManager {
    private readonly incomingStreams = new Map<number, CAMPReadable>();
    private readonly outgoingStreams = new Map<number, AbortController>();

    public constructor(
        private sid: bigint,
        private send: (frame: Buffer) => Promise<void>,
        private next_ack: () => number,
        private next_txid: () => number,
        private destroy: (code?: number, message?: string) => void,
        private get_features: () => bigint
    ) {
        super();
    }

    /**
     * Stream a readable to the client
     * @param source The {@link Readable} object to be streamed
     * @param options The options for streaming the readable
     * */
    public async Stream(source: Readable, options: {
        streamName: string,
        behaviour: "pull" | "push"
    } = {streamName: "anonymous", behaviour: "push"}) {
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
    public async WaitForStream(streamName: string = "anonymous", timeout: number = 2500): Promise<CAMPReadable> {
        const timeoutSig = AbortSignal.timeout(timeout);

        return new Promise<CAMPReadable>((resolve, reject) => {
            const onTxStartListener = async (txId: number, txName: string) => {
                if (txName === streamName) {
                    if (!this.incomingStreams.has(txId)) {
                        this.off("tx-start", onTxStartListener);
                        timeoutSig.removeEventListener("abort", onAbort);

                        reject(new Error(`No stream id ${txId} present!`));
                    }

                    const stream = this.incomingStreams.get(txId)!;

                    //Remove this listener once the stream has been read
                    stream.on("close", () => {
                        this.off("tx-start", onTxStartListener);
                    })

                    resolve(stream);
                }
            }

            const onAbort = () => {
                this.off("tx-start", onTxStartListener);
                timeoutSig.removeEventListener("abort", onAbort);
                reject(new Error(`Timeout elapsed!`));
            }

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
    public async StreamRequestRange(stream: CAMPReadable, start: bigint, end: bigint): Promise<void> {
        const fetch_ack_id = this.next_ack();
        const fetch_frame = TXFetchFrame.Serialize(this.sid, fetch_ack_id, stream.txId, start, end);
        await this.send(fetch_frame);
    }

    /**
     * Cancels a stream
     * @param stream The readable object returned by {@link WaitForStream}
     * */
    public async StreamCancel(stream: CAMPReadable): Promise<void> {
        const encodedCancelFrame = TXCancelFrame.Serialize(this.sid, this.next_ack(), stream.txId);
        stream.destroy();
        await this.send(encodedCancelFrame);
    }

    private async StreamPush(source: Readable, streamName: string): Promise<void> {
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

            for await(const chunk of source as AsyncIterable<Buffer>) {
                signal.throwIfAborted();
                const chunk_frame = TXChunkFrame.Serialize(this.sid, new_txid, offset++, chunk);
                await this.send(chunk_frame);

                offset += BigInt(chunk.byteLength);
            }

            //send tx_finish
            const finish_ack_id = this.next_ack();
            const finish_frame = TXFinishFrame.Serialize(this.sid, finish_ack_id, new_txid);
            await this.send(finish_frame);

        } catch (reason) {
            //this.log(`Transaction ${new_txid} was aborted by client.`);
        } finally {
            this.outgoingStreams.delete(new_txid);
        }
    }

    private PullTransaction = class {
        public byteLength = 0n;
        private path: string = "";
        private handle: FileHandle | null = null;
        private _disposed = false;

        constructor(
            private source: Readable,
            public txId: number,
        ) {
        }

        /**
         * Buffers the stream once on disk in a temporary directory
         * */
        public async setup() {
            this.path = path.join(tmpdir(), `CAMP-TX-${process.pid}-${this.txId}-${crypto.randomUUID()}`);

            //write source to path
            const writeStream = createWriteStream(this.path);
            this.source.pipe(writeStream, {end: true});
            await finished(writeStream);

            const stats = await stat(this.path, {bigint: true});
            this.byteLength = stats.size;

            this.handle = await open(this.path, "r");
        }

        public getRangedStream(start: bigint, end: bigint) {
            Guard.AgainstNullish(this.handle);
            if (end <= start)
                return Readable.from([]);

            return this.handle.createReadStream({start: Number(start), end: Number(end - 1n), autoClose: false});
        }

        public async dispose() {
            if (this._disposed)
                return;

            Guard.AgainstNullish(this.handle);
            await this.handle.close();
            await unlink(this.path);
            this._disposed = true;
        }
    }

    private StreamPull(source: Readable, streamName: string): Promise<void> {
        return new Promise<void>(async (resolve) => {
            const start_ack_id = this.next_ack();
            const new_txid = this.next_txid();

            const controller = new AbortController();
            const signal = controller.signal;
            this.outgoingStreams.set(new_txid, controller);

            const tran = new this.PullTransaction(source, new_txid);
            await tran.setup();

            const handleFetch = async (txId: number, start: bigint, end: bigint) => {
                try {
                    signal.throwIfAborted();

                    const rangeStream = tran.getRangedStream(start, end);
                    let offset = start;
                    for await(const chunk of rangeStream as AsyncIterable<Buffer>) {
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

                } catch (reason) {
                    await cleanup();
                    resolve();
                }
            }

            const cleanup = async () => {
                if (signal.aborted)
                    return;

                controller.abort("cleanup");
                this.outgoingStreams.delete(new_txid);
                await tran.dispose();
            }

            try {
                const start_frame = TXStartFrame.Serialize(this.sid, start_ack_id, new_txid, streamName, tran.byteLength, CAMP_FLOW_BEHAVIOUR.TX_PULL);
                await this.send(start_frame);

                for await(const params of on(this, "tx-fetch", {signal})) {
                    const [txId, start, end] = params as [number, bigint, bigint];
                    if (txId !== new_txid)
                        continue;

                    await handleFetch(txId, start, end);
                }

            } catch (reason) {
                await cleanup();
                resolve();
            }
        });
    }

    public async handle(frame: Buffer) {
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

    private async HandleTxStart(frame: Buffer) {
        if (this.abortIfMismatched())
            return;

        const decodedStartFrame = TXStartFrame
            .Deserialize(frame);

        const stream = new Readable({
            read() {
            }
        });

        const {txId, txName} = decodedStartFrame;

        //Handle stream
        stream.on("close", () => {
            this.incomingStreams.delete(txId);
        });

        Object.defineProperty(stream, "txId", {value: txId});
        this.incomingStreams.set(txId, stream as CAMPReadable);

        await this.acknowledge(decodedStartFrame.ack);

        this.emit("tx-start", txId, txName);
    }

    private async HandleTxCancel(frame: Buffer) {
        if (this.abortIfMismatched())
            return;

        const decodedCancelFrame = TXCancelFrame
            .Deserialize(frame);

        const {txId} = decodedCancelFrame;

        if (!this.outgoingStreams.has(txId))
            return;

        this.outgoingStreams.get(txId)?.abort("Cancelled by client.");
        await this.acknowledge(decodedCancelFrame.ack);
    }

    private async HandleTxFinish(frame: Buffer) {
        if (this.abortIfMismatched())
            return;

        const decodedFinishFrame = TXFinishFrame
            .Deserialize(frame);

        const {txId} = decodedFinishFrame;

        //Handle stream
        if (!this.incomingStreams.has(txId))
            return;
        this.incomingStreams.get(txId)!.push(null);

        await this.acknowledge(decodedFinishFrame.ack);

        this.emit("tx-finish", txId);
    }

    private async HandleTxFetch(frame: Buffer) {
        if (this.abortIfMismatched())
            return;

        const decodedFetchFrame = TXFetchFrame
            .Deserialize(frame);

        await this.acknowledge(decodedFetchFrame.ack);

        this.emit("tx-fetch", decodedFetchFrame.txId, decodedFetchFrame.start, decodedFetchFrame.end);
    }

    private async HandleTxChunk(frame: Buffer) {
        if (this.abortIfMismatched())
            return;

        const decodedChunkFrame = TXChunkFrame
            .Deserialize(frame);

        const {payload, txId} = decodedChunkFrame;

        if (!this.incomingStreams.has(txId))
            return;

        this.incomingStreams.get(txId)!.push(payload);

        this.emit("tx-chunk", txId, payload);
    }

    private async acknowledge(ack_id: number) {
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.send(encodedACKMessage);
    }

    private abortIfMismatched() {
        if (!CAMPHasFeatureFlag(this.get_features(), CAMP_FEATURE_MASK_TRANSACTION)) {
            this.destroy(4002, "PROTOCOL FEATURE MISMATCH - The connected client does not support features in the namespace 'CAMP.Transaction' !");
            return true;
        }

        return false;
    }
}