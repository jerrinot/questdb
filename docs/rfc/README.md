# Local RFC copies

Vendored plain-text RFCs for Stage 2 (HTTP/2) design and review. Keeping
them in-tree means `grep` / `Read` work the same way as on any other
source file, design-doc review cycles don't need network round-trips,
and we're pinned to a specific revision so a later RFC editor tooling
change can't move the goalposts under an open review thread.

| File            | Title                                                      | Referenced by                                |
|-----------------|------------------------------------------------------------|----------------------------------------------|
| `rfc7540.txt`   | HTTP/2 (May 2015, obsoleted by RFC 9113)                   | HTTP2_FRAME_CODEC.md, HPACK_CODEC.md, STREAM_STATE_MACHINE.md |
| `rfc9113.txt`   | HTTP/2 (June 2022, normative)                              | same                                         |
| `rfc7541.txt`   | HPACK: Header Compression for HTTP/2                       | HPACK_CODEC.md                               |

## Usage

Our design docs cite sections by number (e.g. `RFC 9113 sec. 6.9.2`).
Cross-check by opening the corresponding `.txt` and `grep`ping for the
section header; the IETF plain-text format uses lines like:

```
6.9.2.  Initial Flow-Control Window Size
```

so a literal string match (or ripgrep with `-F`) lands on the section
directly. Worked example:

```
$ grep -n '^6\.9\.2\.' docs/rfc/rfc9113.txt
6.9.2.  Initial Flow-Control Window Size
```

## Source

Fetched from the official RFC editor:

- https://www.rfc-editor.org/rfc/rfc7540.txt
- https://www.rfc-editor.org/rfc/rfc9113.txt
- https://www.rfc-editor.org/rfc/rfc7541.txt

No modifications; byte-for-byte copies.
