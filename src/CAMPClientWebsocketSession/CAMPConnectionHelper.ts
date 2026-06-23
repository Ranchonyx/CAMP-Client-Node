import WebSocket from "ws";
import {DebugLoggerFunction} from "node:util";
import {CreateDebugLogger} from "../Common/Util/CreateDebugLogger.js";

export class CAMPConnectionHelper {
    private socket: WebSocket | null = null;
    private readonly url: URL;

    public constructor(
        host: string,
        bearer: string,
        sid: bigint,
        private connectionTimeout: number,
        private maxPayload = 256 * 1024 * 1024,
        private additionalQueryParamsMap: Record<string, string>,
        private log: DebugLoggerFunction = CreateDebugLogger("CRYO_CONNECTION_HELPER")) {

        this.url = new URL(host);
        this.url.searchParams.set("authorization", `Bearer ${bearer}`);
        this.url.searchParams.set("x-cryo-sid", String(sid));

        for (const [key, value] of Object.entries(this.additionalQueryParamsMap)) {
            this.url.searchParams.set(key, value);
        }

    }

    private async ConnectSocket(): Promise<WebSocket> {
        return new Promise<WebSocket>((resolve, reject) => {
            const socket = new WebSocket(this.url, {maxPayload: this.maxPayload, timeout: this.connectionTimeout});
            let currentAttempt = 0;

            socket.on("error", (err) => {
                reject(new Error(`Nonspecific error during WebSocket initialisation`, {cause: err}));
            });

            socket.on("unexpected-response", (_, res) => {
                let body = "";
                res.on("data", (chunk) => body += chunk);
                res.on("end", () => reject(new Error(`Error: Received HTTP status code: ${res.statusCode} / ${body}`)));
            });

            //Reject if we reach the connection timeout
            setTimeout(() => {
                if (this.socket?.readyState !== WebSocket.OPEN)
                    reject(new Error(`Error: Timeout of ${this.connectionTimeout} ms reached during connecting`));
            }, this.connectionTimeout);

            socket.addEventListener("open", () => {
                //Sanity check - I'm paranoid
                socket.removeAllListeners("error");
                socket.removeAllListeners("unexpected-response");

                resolve(socket);
            });
        })
    }

    private async ConnectWithBackoff(
        maxAttempts: number = 5,
        baseDelay: number = 500,
        maxDelay: number = 15000) {
        //Backoff loop
        let currentAttempt = 0;
        while (currentAttempt < maxAttempts) {
            try {
                this.socket = await this.ConnectSocket();
                return;
            } catch (ex) {
                //Should always eval to true, unless I was retarded at some point
                if (ex instanceof Error) {
                    const delay = Math.min(baseDelay * Math.pow(2, currentAttempt), maxDelay);

                    this.log(`Unable to connect to '${this.url}'. Error: ${ex.message}`);
                    this.log(`Retrying connection in ${delay} ms. Attempt ${currentAttempt} / ${maxAttempts}`);

                    await new Promise((resolve) => setTimeout(resolve, delay));
                }
            }
            currentAttempt++;
        }

        //If we got here, we were unable to contact the server. give up!
        throw new Error(`Unable to connect to '${this.url.host}' after ${maxAttempts} attempts. Giving up.`);
    }

    /*
    * Acquires the websocket
    * */
    public async Acquire(): Promise<WebSocket> {
        if (!this.socket)
            return new Promise<WebSocket>((resolve, reject) => {
                this.ConnectWithBackoff(5, 500, 10000).then(_ => {
                    resolve(this.socket!);
                }).catch(err => {
                    reject(err);
                })
            });

        return this.socket;
    }
}