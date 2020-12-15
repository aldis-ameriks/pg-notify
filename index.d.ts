import { ClientConfig } from 'pg'

export interface Options {
  reconnectMaxRetries?: number;
  maxPayloadSize?: number;
  db: ClientConfig;
}

declare class PGPubSub {
  constructor (opts: Options);
  emit (channel: string, payload: any): Promise<void>;
  on (topic: string, listener: (payload: any) => void): Promise<void>;
  removeListener (topic: string, listener: (payload: any) => void): Promise<void>;
  connect (): Promise<void>;
  close (): Promise<void>;
}

export default PGPubSub
