"""Extract immutable ICU 57.1 root weights for duplicate-key diagnostics.

Input: https://raw.githubusercontent.com/unicode-org/icu/release-57-1/icu4c/source/data/unidata/FractionalUCA.txt
No MongoDB captures or candidate runtime participate in this derivation.
"""

import argparse
import hashlib
import json
import re

from pathlib import Path


SOURCE_SHA256 = "73838732a2af47fd2d8ea0f7d326a16c03be0d15a498077756e001992edce04b"
HAN_COUNT = 80388


def extract(source):
    if hashlib.sha256(source).hexdigest() != SOURCE_SHA256:
        message = "expected the unmodified Unicode ICU 57.1 FractionalUCA source"
        raise ValueError(message)
    mappings, contexts, compressible, han = [], [], [], []
    for raw in source.decode("utf-8-sig").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line.startswith("[top_byte") and "COMPRESS" in line:
            compressible.append(int(line.split()[1], 16))
        if line.startswith("[radical ") and ":" in line:
            text = line.split(":", 1)[1].removesuffix("]")
            position = 0
            while position < len(text):
                if position + 2 < len(text) and text[position + 1] == "-":
                    han.extend(
                        chr(cp)
                        for cp in range(
                            ord(text[position]), ord(text[position + 2]) + 1
                        )
                    )
                    position += 3
                else:
                    han.append(text[position])
                    position += 1
        context = re.fullmatch(r"([0-9A-F ]+)\|([0-9A-F ]+);\s*(.*)", line)
        if context:
            contexts.append(
                [
                    "".join(chr(int(value, 16)) for value in context[i].split())
                    for i in (1, 2)
                ]
                + [
                    [
                        [part.strip().replace(" ", "") for part in group.split(",")]
                        for group in re.findall(r"\[([^\]]*)\]", context[3])
                    ]
                ]
            )
        match = re.fullmatch(r"([0-9A-F ]+);\s*(.*)", line)
        if match is None:
            continue
        text = "".join(chr(int(value, 16)) for value in match[1].split())
        weights = [
            [part.strip().replace(" ", "") for part in group.split(",")]
            for group in re.findall(r"\[([^\]]*)\]", match[2])
        ]
        if weights:
            mappings.append([text, weights])
    if len(han) != HAN_COUNT or len(set(han)) != HAN_COUNT:
        message = "unexpected ICU 57.1 Han inventory"
        raise ValueError(message)
    return {
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "uca_version": "8.0.0",
        "icu_version": "57.1",
        "compressible": [*compressible, 254],
        "han": "".join(han),
        "mappings": mappings,
        "contexts": contexts,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("src/mongoeco/core/resources/icu57_root.json"),
    )
    args = parser.parse_args()
    data = extract(args.source.read_bytes())
    args.output.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
