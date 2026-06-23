import {Readable} from "node:stream";
import {CAMP_FLOW_BEHAVIOUR} from "camp-protocol";

export class CAMPReadable extends Readable {
    private receivedBytes = 0n;
    private finished = false;

    public constructor(public txId: number, public byteLength: bigint | null, public behaviour: CAMP_FLOW_BEHAVIOUR) {
        super();
    }

    public _read(size: number) {
    }

    public pushChunk(chunk: Buffer) {
        if (this.finished)
            return;

        this.receivedBytes += BigInt(chunk.byteLength);
        this.push(chunk);
    }

    public finish(): void {
        if (this.finished)
            return;

        this.finished = true;
        this.push(null);
    }

    public getReceivedBytes(): bigint {
        return this.receivedBytes;
    }

    public getRemainingBytes(): bigint | null {
        if (this.byteLength === null)
            return null;

        const remaining = this.byteLength - this.receivedBytes;
        return remaining > 0n ? remaining : 0n;
    }

    public isComplete(): boolean {
        return this.byteLength !== null && this.receivedBytes >= this.byteLength;
    }

    public getProgress(): number | null {
        if (this.byteLength === null || this.byteLength === 0n)
            return null;

        return Number(this.receivedBytes) / Number(this.byteLength);
    }

    public isPull(): boolean {
        return this.behaviour === CAMP_FLOW_BEHAVIOUR.TX_PULL;
    }

    public isPush(): boolean {
        return this.behaviour === CAMP_FLOW_BEHAVIOUR.TX_PUSH;
    }

}