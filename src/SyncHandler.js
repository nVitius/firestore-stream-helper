import BaseWritable from './BaseWritable'

export default class SyncHandler extends BaseWritable {
  constructor(config = { streamOptions: {} }) {
    super({ ...config })
  }

  async _commit(chunk) {
    await this._handler(chunk)
  }
}
