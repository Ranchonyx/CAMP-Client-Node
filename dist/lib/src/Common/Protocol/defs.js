export var BinaryMessageType;
(function (BinaryMessageType) {
    BinaryMessageType[BinaryMessageType["ACK"] = 0] = "ACK";
    BinaryMessageType[BinaryMessageType["ERROR"] = 1] = "ERROR";
    BinaryMessageType[BinaryMessageType["PING_PONG"] = 2] = "PING_PONG";
    BinaryMessageType[BinaryMessageType["UTF8DATA"] = 3] = "UTF8DATA";
    BinaryMessageType[BinaryMessageType["BINARYDATA"] = 4] = "BINARYDATA";
    BinaryMessageType[BinaryMessageType["TX_START"] = 5] = "TX_START";
    BinaryMessageType[BinaryMessageType["TX_CHUNK"] = 6] = "TX_CHUNK";
    BinaryMessageType[BinaryMessageType["TX_FINISH"] = 7] = "TX_FINISH";
})(BinaryMessageType || (BinaryMessageType = {}));
