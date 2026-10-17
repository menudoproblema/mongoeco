import base64, hashlib, json, urllib.request, zipfile
from pathlib import Path

work = Path(__file__).resolve().parent
version = "4.11.0"
def get(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.load(response)
metadata = get(f"https://pypi.org/pypi/mongoeco/{version}/json")
assert metadata["info"]["version"] == version
(work / "pypi-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
expected = json.loads((work / "final-artifacts.json").read_text())
public = work / "public-dist"
public.mkdir(exist_ok=True)
results = []
for item in metadata["urls"]:
    name = item["filename"]
    assert name in expected, name
    with urllib.request.urlopen(item["url"], timeout=60) as response:
        data = response.read()
    digest = hashlib.sha256(data).hexdigest()
    assert digest == item["digests"]["sha256"] == expected[name], name
    assert data == (work / "final-main-dist" / name).read_bytes()
    assert data == (work / "tag-dist" / name).read_bytes()
    (public / name).write_bytes(data)
    url = f"https://pypi.org/integrity/mongoeco/{version}/{name}/provenance"
    provenance = get(url)
    (work / (name + ".provenance.json")).write_text(json.dumps(provenance, indent=2) + "\n")
    publishers = []
    matched = False
    for bundle in provenance["attestation_bundles"]:
        publisher = bundle["publisher"]
        assert publisher["repository"] == "menudoproblema/mongoeco"
        assert publisher["workflow"] == "ci.yml"
        assert publisher["environment"] == "pypi"
        publishers.append(publisher)
        for attestation in bundle["attestations"]:
            statement = json.loads(base64.b64decode(attestation["envelope"]["statement"]))
            for subject in statement["subject"]:
                if subject["name"] == name and subject["digest"]["sha256"] == digest:
                    matched = True
    assert matched, name
    results.append({"filename": name, "sha256": digest, "url": item["url"],
                    "upload_time": item["upload_time_iso_8601"],
                    "identical_to_main_and_tag_ci": True, "publishers": publishers,
                    "provenance_url": url, "attestation_subject_matches": matched})
assert len(results) == len(expected) == 2
wheel = next(public.glob("*.whl"))
with zipfile.ZipFile(wheel) as archive:
    count = 0
    for name in archive.namelist():
        if name.startswith("mongoeco/"):
            assert archive.read(name) == (work.parents[1] / "src" / name).read_bytes(), name
            count += 1
(work / "public-artifacts.json").write_text(json.dumps({"version": version,
    "artifacts": results, "runtime_files_match_source": count}, indent=2) + "\n")
print(f"PyPI, main and tag distributions identical; publisher and subjects verified; {count} runtime files match")
