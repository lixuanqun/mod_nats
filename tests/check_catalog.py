#!/usr/bin/env python3
"""Wire-catalog drift check for mod_nats.

Compares three sources of truth that must agree:
  1. the method table in nats_methods.c (canonical fs.* names),
  2. the method/event defs in schema/catalog.json,
  3. the protocol version strings in mod_nats.h and the catalog description.

Also verifies every canonical method is documented in README.md. Exits 1 on
any drift; silent (exit 0) when the sources agree.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Events the module always publishes (routing may send them to public
# subjects, mailboxes, or both) - each must have a catalog def.
ALWAYS_PUBLISHED_EVENTS = [
    "Event.Channel", "Event.CDR", "Event.Result", "Event.OwnerLost",
    "Event.NodeUp", "Event.Metrics", "Event.Detected", "Event.NativeEvent",
]


def c_method_table():
    src = (ROOT / "nats_methods.c").read_text(encoding="utf-8")
    m = re.search(r"const mod_nats_method_t mod_nats_methods\[\]\s*=\s*\{(.*?)\n\};", src, re.S)
    if not m:
        sys.exit("FAIL: cannot locate mod_nats_methods[] in nats_methods.c")
    rows = re.findall(r'\{"([^"]+)"\s*,\s*(?:NULL|"([^"]*)")', m.group(1))
    methods = [name for name, _alias in rows if name != "NULL"]
    aliases = {alias for _name, alias in rows if alias}
    return methods, aliases


def c_proto_version():
    hdr = (ROOT / "mod_nats.h").read_text(encoding="utf-8")
    m = re.search(r'#define MOD_NATS_PROTO_VERSION "([^"]+)"', hdr)
    if not m:
        sys.exit("FAIL: cannot locate MOD_NATS_PROTO_VERSION in mod_nats.h")
    return m.group(1)


def catalog():
    path = ROOT / "schema" / "catalog.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        sys.exit(f"FAIL: schema/catalog.json is not valid JSON: {e}")


def main():
    failed = False

    def check(name, ok, detail=""):
        nonlocal failed
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")
        failed = failed or not ok

    methods, aliases = c_method_table()
    cat = catalog()
    defs = cat.get("$defs", {})
    cat_methods = {k for k in defs if k.startswith("fs.")}

    check(f"C table: {len(methods)} methods, {len(aliases)} XCC aliases", len(methods) > 0)
    check("catalog valid and parseable", True)

    missing_in_cat = sorted(set(methods) - cat_methods)
    stale_in_cat = sorted(cat_methods - set(methods))
    check("every C method has a catalog def", not missing_in_cat, str(missing_in_cat))
    check("no stale catalog methods", not stale_in_cat, str(stale_in_cat))

    for ev in ALWAYS_PUBLISHED_EVENTS:
        check(f"catalog def exists for {ev}", ev in defs)

    version = c_proto_version()
    m = re.search(r"protocol (\S+)\.", cat.get("description", ""))
    check("catalog description carries the current proto version",
          bool(m) and m.group(1) == version,
          f"catalog={m.group(1) if m else None} mod_nats.h={version}")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    undocumented = [name for name in methods if name not in readme]
    check("every C method is documented in README.md", not undocumented, str(undocumented))

    if failed:
        sys.exit(1)
    print(f"\ncatalog check OK ({len(methods)} methods, proto {version})")


if __name__ == "__main__":
    main()
