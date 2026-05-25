import {CryoClientWebsocketSession} from "./CryoClientWebsocketSession/CryoClientWebsocketSession.js";

/**
 * Create a Cryo server and attach it to an Express.js app
 * @param host - The host to connect to
 * @param bearer - The bearer token to authenticate with at the server
 * @param additionalQueryParamsMap - A record of additional parameters to be appended to the query string in the Upgrade request
 * @param timeout - How long to wait until disconnecting
 * @param maxPayloadReceived - The maximum size of receivable payloads in bytes
 * */
export async function cryo(host: string, bearer: string, additionalQueryParamsMap: Record<string, string>, timeout: number = 5000, maxPayloadReceived = 256 * 1024 * 1024) {
    return CryoClientWebsocketSession.Connect(host, bearer, additionalQueryParamsMap, timeout, maxPayloadReceived)
}