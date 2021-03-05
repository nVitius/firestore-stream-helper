import { Writable } from 'stream'

export default class BaseWritable extends Writable {
  constructor(
    config = { writev: false, streamOptions: {} }
  ) {
    super({ objectMode: true, ...config.streamOptions})

    if (Object.getPrototypeOf(this) === BaseWritable.prototype) {
      throw new Error('BaseWritable should be extended')
    }

    this.config = config
  }

  async _write(chunk, enc, next) {
    await this._commit(chunk)
    next()
  }

  async _writev(chunks, next) {
    if (this.config.writev === true)
      await this._commit(chunks)
    else
      await Promise.all(
        chunks.map(obj => this._commit(obj.chunk))
      )

    next()
  }

  async _commit() {
    throw new Error('method not implemented')
  }

  async _handler(chunk) {
    if (this.handler === undefined)
      throw new Error('method not implemented')

    return await this.handler(chunk)
  }
}
