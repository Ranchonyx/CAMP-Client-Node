export class AckTracker {
    pending = new Map();
    ewma_rtt = null;
    alpha = 0.2;
    Track(ack, message) {
        this.pending.set(ack, message);
    }
    Confirm(ack) {
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
    Has(ack) {
        return this.pending.has(ack);
    }
    get rtt() {
        return this.ewma_rtt || -1;
    }
}
