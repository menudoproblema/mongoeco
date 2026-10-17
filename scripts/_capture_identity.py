"""Reject evidence whose source inputs change during an observation."""

import hashlib


class SourceIdentity:
    def __init__(self, root, paths):
        self.root = root
        self.hashes = self._hashes(paths)

    def _hashes(self, paths):
        return {
            path: hashlib.sha256((self.root / path).read_bytes()).hexdigest()
            for path in paths
        }

    def verify(self):
        if self._hashes(self.hashes) != self.hashes:
            message = "capture sources changed during execution; evidence rejected"
            raise RuntimeError(message)
        return dict(self.hashes)
