import { Readable } from "node:stream";
import { CAMP_FLOW_BEHAVIOUR } from "camp-protocol";
export class CAMPReadable extends Readable {
    txId;
    byteLength;
    behaviour;
    receivedBytes = 0n;
    finished = false;
    constructor(txId, byteLength, behaviour) {
        super();
        this.txId = txId;
        this.byteLength = byteLength;
        this.behaviour = behaviour;
    }
    _read(size) {
    }
    pushChunk(chunk) {
        if (this.finished)
            return;
        this.receivedBytes += BigInt(chunk.byteLength);
        this.push(chunk);
    }
    finish() {
        if (this.finished)
            return;
        this.finished = true;
        this.push(null);
    }
    getReceivedBytes() {
        return this.receivedBytes;
    }
    getRemainingBytes() {
        if (this.byteLength === null)
            return null;
        const remaining = this.byteLength - this.receivedBytes;
        return remaining > 0n ? remaining : 0n;
    }
    isComplete() {
        return this.byteLength !== null && this.receivedBytes >= this.byteLength;
    }
    getProgress() {
        if (this.byteLength === null || this.byteLength === 0n)
            return null;
        return Number(this.receivedBytes) / Number(this.byteLength);
    }
    isPull() {
        return this.behaviour === CAMP_FLOW_BEHAVIOUR.TX_PULL;
    }
    isPush() {
        return this.behaviour === CAMP_FLOW_BEHAVIOUR.TX_PUSH;
    }
}
