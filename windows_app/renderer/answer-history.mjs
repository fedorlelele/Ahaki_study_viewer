// The answer and its outbox entry are saved in the same localStorage write.
// Import old selections for display. They do not have a known answer time.
const key = 'asv.windows.answer-history.v2';
const oldKey = 'asv.windows.answers.v1';

export function createAnswerHistory({storage, uuid, upload, notify, reportError}) {
  function load(key) {
    try {
      const value = JSON.parse(storage.getItem(key));
      return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
    } catch {
      reportError('保存済みの回答履歴を読み込めません。');
      return null;
    }
  }
  const saved = load(key);
  const state = saved?.anonId && saved.choices && Array.isArray(saved.pending) ? saved :
    {anonId: uuid(), choices: load(oldKey) || {}, pending: []};
  let flushing = false;
  function persist() {
    try {
      storage.setItem(key, JSON.stringify(state));
      return true;
    } catch {
      reportError('回答履歴を端末に保存できません。端末の空き容量を確認してください。');
      return false;
    }
  }
  async function flush() {
    if (flushing || !state.pending.length) return;
    flushing = true;
    try {
      while (state.pending.length) {
        const batch = state.pending.slice(0, 100);
        await upload(state.anonId, batch);
        // New answers may have been appended while this request was in flight.
        state.pending.splice(0, batch.length);
        if (!persist()) break;
      }
    } catch {
      // Keep unacknowledged attempts, including after an ambiguous timeout.
    } finally {
      flushing = false;
      notify(state.pending.length);
    }
  }
  return {
    choices: state.choices,
    flush,
    get pendingCount() { return state.pending.length; },
    record(serial, number, correct) {
      state.choices[serial] = number;
      state.pending.push({client_event_id: uuid(), serial, selected_index: number,
        is_correct: correct, created_at: new Date().toISOString()});
      persist();
      notify(state.pending.length);
      void flush();
    },
    retry(serial) {
      delete state.choices[serial];
      persist();
    }
  };
}
