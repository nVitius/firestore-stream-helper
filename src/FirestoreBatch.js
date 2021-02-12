import { Writable } from 'stream'

export default class FirestoreBatch extends Writable {
  constructor(
    firestore,
    config = { limit: 500, streamOptions: {} }
  ) {
    super({ objectMode: true, highWaterMark: config.limit, ...config.streamOptions})

    this.firestore = firestore
    this.config = config

    this.queue = []
  }

  async _write(chunk, enc, next) {
    await this._queue(chunk)
    next()
  }

  async _writev(chunks, next) {
    await this._queue(...chunks.map(obj => obj.chunk))
    next()
  }

  async _final(next) {
    if (this.queue.length > 0)
      await this._commit()

    next()
  }

  async _queue(...chunks) {
    const max = this.config.limit - this.queue.length

    if (chunks.length > max) {
      // fill up queue and process
      await this._queue(chunks.slice(0, max - chunks.length))

      // process remainder
      return this._queue(chunks.slice(max-chunks.length))
    }

    this.queue.push(...chunks)
    if (this.queue.length === this.config.limit)
      await this._commit()
  }

  async _commit() {
    const batch = this.firestore.batch()
    await Promise.all(this.queue.map(x => {
      return this._handler(x, batch)
    }))

    await batch.commit()
    this.queue = []
  }

  _handler(docSnap, batch) {
    if (this.handler === undefined)
      return Promise.reject('method not implemented')

    return this.handler(docSnap, batch)
  }
}

export const Handlers = {
  delete: (docSnap, batch) => {
    batch.delete(docSnap.ref)
  }
}
