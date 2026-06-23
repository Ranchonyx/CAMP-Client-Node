import {CAMPClientWebsocketSession} from "./CAMPClientWebsocketSession/CAMPClientWebsocketSession.js";

/**
 * Create a CAMP client
 * @param host - The host to connect to
 * @param bearer - The bearer token to authenticate with at the server
 * @param timeout - How long to wait until disconnecting
 * @param additionalQueryParamsMap - A record of additional parameters to be appended to the query string in the upgrade request
 * @param maxPayloadReceived - The maximum size of receivable payloads in bytes
 * */
export async function camp(host: string, bearer: string, timeout: number = 5000, additionalQueryParamsMap: Record<string, string>, maxPayloadReceived = 256 * 1024 * 1024) {
    return CAMPClientWebsocketSession.Connect(host, bearer, additionalQueryParamsMap, timeout, maxPayloadReceived)
}