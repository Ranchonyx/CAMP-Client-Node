import {
    ACKFrame, BinaryDataFrame,
    CAMPFrameType,
    BufferUtil,
    ByeFrame,
    CAMP_PROTOCOL_VERSION,
    EndpointInfoFrame, ErrorFrame, PingPongFrame, Utf8DataFrame
} from "camp-protocol";
import {EventEmitter} from "node:events";
import {AckTracker} from "../../Common/AckTracker/AckTracker.js";

interface CAMPBaseManagerEvents {
    "ready": () => void;
    "message-utf8": (message: string) => void;
    "message-binary": (message: Buffer) => void;
    "message-error": (message: string) => void;
}

export interface CAMPBaseManager {
    on<U extends keyof CAMPBaseManagerEvents>(event: U, listener: CAMPBaseManagerEvents[U]): this;

    emit<U extends keyof CAMPBaseManagerEvents>(event: U, ...args: Parameters<CAMPBaseManagerEvents[U]>): boolean;
}

export class CAMPBaseManager extends EventEmitter implements CAMPBaseManager {
    public constructor(
        private sid: bigint,
        private send: (frame: Buffer) => Promise<void>,
        private next_ack: () => number,
        private destroy: (code?: number, message?: string) => void,
        private set_features: (features: bigint) => void,
        private client_ack_tracker: AckTracker
    ) {
        super();
    }

    /**
     * Send a ping to the client
     * */
    public async Ping(): Promise<void> {
        const encodedPingMessage = PingPongFrame
            .Serialize(this.sid, "ping");

        await this.send(encodedPingMessage);
    }

    /**
     * Send a UTF8 message to the client
     * */
    public async SendUTF8(message: string): Promise<void> {
        const encodedUtf8DataMessage = Utf8DataFrame
            .Serialize(this.sid, this.next_ack(), message);

        await this.send(encodedUtf8DataMessage);
    }

    /**
     * Send a binary Message to the client
     * */
    public async SendBinary(message: Buffer): Promise<void> {
        const encodedBinaryDataMessage = BinaryDataFrame
            .Serialize(this.sid, this.next_ack(), message);

        await this.send(encodedBinaryDataMessage);
    }

    /**
     * Send an error message to the client
     * */
    public async SendError(message: string): Promise<void> {
        const encodedErrorMessage = ErrorFrame.Serialize(this.sid, this.next_ack(), message);

        await this.send(encodedErrorMessage);
    }

    public async handle(frame: Buffer) {
        const type = BufferUtil.GetType(frame);

        switch (type) {
            case CAMPFrameType.ENDPOINT_INFO:
                await this.HandleEndpointInfo(frame);
                return;
            case CAMPFrameType.BYE:
                await this.HandleBye(frame);
                return;
            case CAMPFrameType.ACK:
                await this.HandleAck(frame);
                return;
            case CAMPFrameType.ERROR:
                await this.HandleError(frame);
                return;
            case CAMPFrameType.PING_PONG:
                await this.HandlePingPong(frame);
                return;
            case CAMPFrameType.UTF8DATA:
                await this.HandleUtf8Data(frame);
                return;
            case CAMPFrameType.BINARYDATA:
                await this.HandleBinaryData(frame);
                return;
        }
    }

    private async HandleBye(frame: Buffer): Promise<void> {
        const decodedByeMessage = ByeFrame
            .Deserialize(frame);

        await this.acknowledge(decodedByeMessage.ack);

        this.destroy(4000, decodedByeMessage.reason);
    }

    private async HandleEndpointInfo(frame: Buffer): Promise<void> {
        const decodedInfoMessage = EndpointInfoFrame
            .Deserialize(frame);

        //Check protocol version equality and fail otherwise
        if (CAMP_PROTOCOL_VERSION !== decodedInfoMessage.version) {
            this.destroy(4001, `Protocol mismatch. Client offered ${decodedInfoMessage.version}, we support ${CAMP_PROTOCOL_VERSION} !`);
            return;
        }

        await this.acknowledge(decodedInfoMessage.ack);

        this.set_features(decodedInfoMessage.features);
        this.emit("ready");
    }

    private async HandlePingPong(frame: Buffer): Promise<void> {
        const decodedPingPongMessage = PingPongFrame
            .Deserialize(frame);

        //A peer is pinging us, play nice and respond
        if (decodedPingPongMessage.payload === "ping") {
            const outgoingPong = PingPongFrame.Serialize(this.sid, "pong");
            await this.send(outgoingPong);
        } else {
            //A normal client responded to our ping
        }
    }

    private async HandleError(frame: Buffer): Promise<void> {
        const decodedErrorMessage = ErrorFrame
            .Deserialize(frame);

        await this.acknowledge(decodedErrorMessage.ack);
        this.emit("message-error", decodedErrorMessage.payload);
    }

    private async HandleAck(frame: Buffer): Promise<void> {
        const decodedAckMessage = ACKFrame
            .Deserialize(frame);

        const {ack} = decodedAckMessage;
        const found_message = this.client_ack_tracker.Confirm(ack);

        if (!found_message) {
            return;
        }

        found_message.ackPromise?.resolve();
    }

    private async HandleUtf8Data(frame: Buffer): Promise<void> {
        const decodedDataMessage = Utf8DataFrame
            .Deserialize(frame);

        await this.acknowledge(decodedDataMessage.ack);
        this.emit("message-utf8", decodedDataMessage.payload);
    }

    private async HandleBinaryData(frame: Buffer): Promise<void> {
        const decodedDataMessage = BinaryDataFrame
            .Deserialize(frame);

        await this.acknowledge(decodedDataMessage.ack);
        this.emit("message-binary", decodedDataMessage.payload);
    }

    private async acknowledge(ack_id: number) {
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);

        await this.send(encodedACKMessage);
    }
}