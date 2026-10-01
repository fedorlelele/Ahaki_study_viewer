"""Prove source receipt from actual CLI stdout, including later shell failures.

This verifies immutable full text, not the model's medical conclusion. Nonzero
commands qualify only when a known read helper was the first shell operation and
its complete first JSON response exactly matches the frozen source files.
"""
import hashlib
import json
import re
from pathlib import Path
import shlex


def _sha(data):
    return hashlib.sha256(data).hexdigest()


class SourceReadProof:
    def __init__(self, index_path, helpers=(), bound=None):
        self.index = Path(index_path).resolve()
        self.base = self.index.parent
        raw = self.index.read_bytes()
        self.index_sha256 = _sha(raw)
        self.units = {u['id']: u for u in map(json.loads, raw.decode().splitlines())}
        self.helpers = {str(Path(p).resolve()) for p in helpers}
        self.bound = bound if bound is not None else {}
        self.bound[str(self.index)] = self.index_sha256
        self.cache = {}

    def validate_run_index(self, metadata, legacy_roster=None):
        """Never invent absent runtime hashes; permit only frozen old records."""
        if 'source_index_sha256' in metadata:
            if metadata['source_index_sha256'] != self.index_sha256:
                raise ValueError('Source index differs from actual session metadata')
            return 'Recorded runtime whole-index SHA matches frozen corpus'
        if legacy_roster is None:
            raise ValueError('New run lacks required runtime source-index SHA')
        path = Path(legacy_roster).resolve()
        raw = path.read_bytes()
        roster = json.loads(raw)
        fingerprint = _sha(json.dumps(metadata, ensure_ascii=False,
            sort_keys=True, separators=(',', ':')).encode('utf-8'))
        matches = [entry for entry in roster['entries']
                   if entry['metadata_canonical_sha256'] == fingerprint]
        if len(matches) != 1:
            raise ValueError('Unregistered or changed legacy run missing source-index SHA')
        entry = matches[0]
        if any(entry[key] != metadata.get(key) for key in ('log_sha256', 'session_id', 'started_at', 'runner_sha256')):
            raise ValueError('Legacy original run identity differs')
        self.bound[str(path)] = _sha(raw)
        return ('Runtime whole-index SHA unrecorded in fixed legacy metadata; '
                'cited full source receipts must match immutable corpus bytes and hashes')

    def expected(self, source_id):
        unit = self.units[source_id]
        path = (self.base / unit['file']).resolve()
        if not path.is_relative_to(self.base):
            raise ValueError('Source path outside frozen corpus')
        # Stat-based cache is only an optimization. Publication binds the actual
        # file bytes, and rejects subsequent mutations before applying a plan.
        stat = path.stat()
        key = (str(path), stat.st_mtime_ns, stat.st_size)
        if key not in self.cache:
            raw = path.read_bytes()
            if _sha(raw) != unit['md_sha256']:
                raise ValueError('Immutable source file SHA differs: ' + str(path))
            self.cache[key] = raw.decode('utf-8').replace('\r\n', '\n').replace('\r', '\n').splitlines(keepends=True)
        lines = self.cache[key]
        text = ''.join(lines[unit['line_start'] - 1:unit['line_end']])
        self.bound[str(path)] = unit['md_sha256']
        return unit, text, _sha(text.encode('utf-8'))

    def matches(self, obj, require_origin_fields=False):
        if not isinstance(obj, dict) or obj.get('id') not in self.units:
            return False
        unit, text, excerpt_sha = self.expected(obj['id'])
        return (obj.get('text') == text and obj.get('excerpt_sha256') == excerpt_sha
                and all(obj.get(k) == unit[k] for k in ('book', 'edition'))
                and all((k in obj or not require_origin_fields) and
                        (k not in obj or obj[k] == unit[k]) for k in ('file', 'md_sha256'))
                and obj.get('location', obj.get('headings')) == unit['headings'])

    @staticmethod
    def objects(output, first_only=False):
        decoder = json.JSONDecoder()
        if first_only:
            try:
                obj, _ = decoder.raw_decode(output.lstrip())
                return [obj]
            except (ValueError, TypeError):
                return []
        objects = []
        consumed_until = 0
        # Decode entire top-level JSON values, never a quoted JSON-looking snippet.
        for line_start in [0] + [i + 1 for i, ch in enumerate(output) if ch == '\n']:
            if line_start < consumed_until:
                continue
            candidate = output[line_start:].lstrip()
            if not candidate.startswith(('{', '[', '"')):
                continue
            try:
                obj, end = decoder.raw_decode(candidate)
                consumed_until = len(output) - len(candidate) + end
                if isinstance(obj, (dict, list)):
                    objects.append(obj)
            except ValueError:
                # An incomplete enclosing value cannot prove that an inner
                # object was received as a complete top-level response.
                break
        return objects

    def leading_read(self, command):
        try:
            outer = shlex.split(command)
            if len(outer) == 3 and Path(outer[0]).name in {'bash', 'zsh', 'sh'} and outer[1] in {'-lc', '-c'}:
                command = outer[2]
            lexer = shlex.shlex(command, posix=True, punctuation_chars=';&|<>')
            lexer.whitespace_split = True
            tokens = list(lexer)
            stop = next((i for i, tok in enumerate(tokens) if tok in {';', '&&'}), len(tokens))
            first = tokens[:stop]
            if len(first) < 4 or not Path(first[0]).name.startswith('python'):
                return None
            if any(any(ch in tok for ch in '$`') or tok in {'|', '||', '&', '>', '<', '>>'} for tok in first):
                return None
            helper = str(Path(first[1]).resolve())
            if helper not in self.helpers:
                return None
            if first[2] == 'read' and all(s in self.units for s in first[3:]):
                self.bound[helper] = _sha(Path(helper).read_bytes())
                return set(first[3:])
            # Search without --full emits only previews and cannot prove receipt.
            if first[2] == 'search' and '--full' in first[3:]:
                self.bound[helper] = _sha(Path(helper).read_bytes())
                return set(self.units)
        except (ValueError, OSError):
            pass
        return None

    def collect(self, events):
        proved = set()
        for event in events:
            item = event.get('item', {})
            if event.get('type') != 'item.completed' or item.get('type') != 'command_execution':
                continue
            if item.get('exit_code') is None:
                continue
            nonzero = item['exit_code'] != 0
            allowed = self.leading_read(item.get('command', '')) if nonzero else None
            if nonzero and allowed is None:
                continue
            for obj in self.objects(item.get('aggregated_output', ''), first_only=nonzero):
                if isinstance(obj, dict):
                    sources = obj.get('sources', [obj])
                elif isinstance(obj, list) and not nonzero:
                    sources = obj
                else:
                    sources = []
                for source in sources:
                    if isinstance(source, dict) and (not nonzero or source.get('id') in allowed) and self.matches(source, require_origin_fields=nonzero):
                        proved.add((source['id'], source['excerpt_sha256']))
        return proved

    @staticmethod
    def validate_hash_fields(value):
        """Reject malformed claimed SHA-256 values, including web references."""
        if isinstance(value, dict):
            for key, item in value.items():
                if key == 'sha256' or key.endswith('_sha256'):
                    if not isinstance(item, str) or not re.fullmatch(r'[0-9a-fA-F]{64}', item):
                        raise ValueError('Malformed reference SHA-256: ' + key)
                else:
                    SourceReadProof.validate_hash_fields(item)
        elif isinstance(value, list):
            for item in value:
                SourceReadProof.validate_hash_fields(item)

    def require(self, references, proved):
        self.validate_hash_fields(references)
        for ref in references:
            if ref.get('type') == 'textbook' and (ref['source_id'], ref['excerpt_sha256']) not in proved:
                raise ValueError('Cited complete source not actually received: ' + ref['source_id'])
