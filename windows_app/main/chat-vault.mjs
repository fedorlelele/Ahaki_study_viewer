import { readFileSync, writeFileSync, mkdirSync, renameSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { ChatError } from './chat-protocol.mjs'

const unavailable = '保存したChatGPTの接続情報を読み書きできません。このWindowsユーザーのアカウントで起動してください。'

export function createVaultStore(directory, storage) {
  const filename = join(directory, 'chat-accounts.bin')
  return {
    load() {
      let data
      try { data = readFileSync(filename) }
      catch (error) {
        if (error.code === 'ENOENT') return {version:1, host:'urn:uuid:' + randomUUID(), profiles:[], active:null, pending:null}
        throw new ChatError(unavailable)
      }
      try {
        if (!storage.isEncryptionAvailable()) throw new Error()
        const vault = JSON.parse(storage.decryptString(data))
        if (vault.version !== 1 || typeof vault.host !== 'string' || !Array.isArray(vault.profiles)) throw new Error()
        return vault
      } catch { throw new ChatError(unavailable) }
    },
    save(vault) {
      const staging = filename + '.' + randomUUID() + '.tmp'
      try {
        if (!storage.isEncryptionAvailable()) throw new Error()
        const ciphertext = storage.encryptString(JSON.stringify(vault))
        mkdirSync(directory, {recursive:true})
        writeFileSync(staging, ciphertext, {mode:0o600, flag:'wx', flush:true})
        renameSync(staging, filename)
      } catch { throw new ChatError(unavailable) }
      finally { try { rmSync(staging, {force:true}) } catch { /* Keep the original record intact. */ } }
    }
  }
}
