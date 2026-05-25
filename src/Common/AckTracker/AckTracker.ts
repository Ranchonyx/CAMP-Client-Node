type PendingBinaryMessage = {
    timestamp: number;
    message: Buffer;
    payload?: string | Buffer;
}

export class AckTracker {
    private pending = new Map<number, PendingBinaryMessage>();
    private ewma_rtt: number | null = null;
    private alpha = 0.2;

    public Track(ack: number, message: PendingBinaryMessage) {
        this.pending.set(ack, message);
    }

    public Confirm(ack: number): PendingBinaryMessage | null {
        const maybe_ack = this.pending.get(ack);
        if (!maybe_ack)
            return null;

        const rtt = Date.now() - maybe_ack.timestamp;
        if (!this.ewma_rtt)
            this.ewma_rtt = rtt;
        else
            this.ewma_rtt = (1 - this.alpha) * this.ewma_rtt + this.alpha * rtt;

        this.pending.delete(ack);
        return maybe_ack;
    }

    public Has(ack: number): boolean {
        return this.pending.has(ack);
    }

    public get rtt(): number {
        return this.ewma_rtt || -1;
    }
}
