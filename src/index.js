import { default as FirestoreBatch, Handlers as FirestoreBatchHandlers } from './FirestoreBatch'
import SyncHandler from './SyncHandler'

export default class FirestoreStreamHelper {
  constructor(firestore) {
    this.firestore = firestore

    this.completed = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }

  from(stream) {
    this.readable = stream

    return this
  }

  withBatchHandler({ limit, streamOptions, handler, handlerOpts }) {
    if (typeof handler === 'string') {
      switch (handler) {
        case 'delete':
          handler = FirestoreBatchHandlers.delete
      }
    }

    this.writable = new FirestoreBatch(this.firestore, { limit, streamOptions })
    this.writable.handler = handler

    return this
  }

  withSyncHandler({ streamOptions, handler, mode = 'each' }) {
    if (mode === 'each')
      this.writable = new SyncHandler(streamOptions)

    this.writable.handler = handler

    return this
  }

  stream() {
    if (!this.readable)
      throw new Error('No stream to read from. Set it with `.from(stream)`')

    if (!this.writable)
      throw new Error('No stream to write to.')

    this.writable.on('finish', () => {
      this.resolve()
    })

    this.writable.on('error', err => {
      this.reject(err)
    })

    this.readable.pipe(this.writable)

    return this
  }

  abort(force = false) {
    if (this.readable) {
      this.readable.destroy()
    }

    if (this.writable) {
      if (force)
        this.writable.destroy()
      else
        this.writable.end()
    }

    this.reject()
  }

  finish() {
    return this.completed
  }
}

