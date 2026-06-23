import { ACKFrame, BinaryDataFrame, CAMPFrameType, BufferUtil, ByeFrame, CAMP_PROTOCOL_VERSION, EndpointInfoFrame, ErrorFrame, PingPongFrame, Utf8DataFrame } from "camp-protocol";
import { EventEmitter } from "node:events";
export class CAMPBaseManager extends EventEmitter {
    sid;
    send;
    next_ack;
    destroy;
    set_features;
    client_ack_tracker;
    constructor(sid, send, next_ack, destroy, set_features, client_ack_tracker) {
        super();
        this.sid = sid;
        this.send = send;
        this.next_ack = next_ack;
        this.destroy = destroy;
        this.set_features = set_features;
        this.client_ack_tracker = client_ack_tracker;
    }
    /**
     * Send a ping to the client
     * */
    async Ping() {
        const encodedPingMessage = PingPongFrame
            .Serialize(this.sid, "ping");
        await this.send(encodedPingMessage);
    }
    /**
     * Send a UTF8 message to the client
     * */
    async SendUTF8(message) {
        const encodedUtf8DataMessage = Utf8DataFrame
            .Serialize(this.sid, this.next_ack(), message);
        await this.send(encodedUtf8DataMessage);
    }
    /**
     * Send a binary Message to the client
     * */
    async SendBinary(message) {
        const encodedBinaryDataMessage = BinaryDataFrame
            .Serialize(this.sid, this.next_ack(), message);
        await this.send(encodedBinaryDataMessage);
    }
    /**
     * Send an error message to the client
     * */
    async SendError(message) {
        const encodedErrorMessage = ErrorFrame.Serialize(this.sid, this.next_ack(), message);
        await this.send(encodedErrorMessage);
    }
    async handle(frame) {
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
    async HandleBye(frame) {
        const decodedByeMessage = ByeFrame
            .Deserialize(frame);
        await this.acknowledge(decodedByeMessage.ack);
        this.destroy(4000, decodedByeMessage.reason);
    }
    async HandleEndpointInfo(frame) {
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
    async HandlePingPong(frame) {
        const decodedPingPongMessage = PingPongFrame
            .Deserialize(frame);
        //A peer is pinging us, play nice and respond
        if (decodedPingPongMessage.payload === "ping") {
            const outgoingPong = PingPongFrame.Serialize(this.sid, "pong");
            await this.send(outgoingPong);
        }
        else {
            //A normal client responded to our ping
        }
    }
    async HandleError(frame) {
        const decodedErrorMessage = ErrorFrame
            .Deserialize(frame);
        await this.acknowledge(decodedErrorMessage.ack);
        this.emit("message-error", decodedErrorMessage.payload);
    }
    async HandleAck(frame) {
        const decodedAckMessage = ACKFrame
            .Deserialize(frame);
        const { ack } = decodedAckMessage;
        const found_message = this.client_ack_tracker.Confirm(ack);
        if (!found_message) {
            return;
        }
        found_message.ackPromise?.resolve();
    }
    async HandleUtf8Data(frame) {
        const decodedDataMessage = Utf8DataFrame
            .Deserialize(frame);
        await this.acknowledge(decodedDataMessage.ack);
        this.emit("message-utf8", decodedDataMessage.payload);
    }
    async HandleBinaryData(frame) {
        const decodedDataMessage = BinaryDataFrame
            .Deserialize(frame);
        await this.acknowledge(decodedDataMessage.ack);
        this.emit("message-binary", decodedDataMessage.payload);
    }
    async acknowledge(ack_id) {
        const encodedACKMessage = ACKFrame
            .Serialize(this.sid, ack_id);
        await this.send(encodedACKMessage);
    }
}
