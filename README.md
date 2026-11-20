# fineweb2-nl-optout

Code that marks, in the Dutch part of [FineWeb-2](https://huggingface.co/datasets/HuggingFaceFW/fineweb-2) (config
`nld_Latn`, revision `af9c133`), every document whose site has reserved text and data mining in a machine-readable
way, or that comes from an infringing site, and that replaces more personal data than FineWeb-2 replaces. The
documents without such a mark are meant as pretraining data for Dutch encoder models.

Every step is a fixed rule, written down and tested before the documents were filtered; every document keeps its
decision with its reasons, and every host a dated record of what its site said.

## The rule

Documents whose crawl (column `dump`) is later than `CC-MAIN-2023-23` are left out. Every other document is written
out with its decision: `optout` is true when any of these reasons holds, and `optout_reasons` lists every reason that
holds, in this order; the first is the one it is counted under (`rules.py`).

| reason | what |
|---|---|
| `robots_star` | its path plus query is disallowed by the host's current `robots.txt` (RFC 9309) for `*` |
| `robots_ai` | the same for an agent of the core list (`CORE_AGENTS`) that the `robots.txt` names |
| `robots_ai_extended` | the same for an agent of the ai.robots.txt list that is not in the core list |
| `robots_signal` | the `robots.txt` group for `*` or a listed agent has `Content-Signal: ai-train=no`, or a `Content-Usage` line (IETF AI Preferences drafts) that withholds training for its path |
| `tdmrep` | `/.well-known/tdmrep.json` (W3C TDMRep) reserves its path: the first rule whose location matches has `tdm-reservation` 1 |
| `aitxt` | the host's `/ai.txt` (Spawning) disallows its path, or disallows text (`*.html` or `*.txt`), for `*` or a listed agent |
| `apertus` | its host is in the Apertus robots.txt blocklist ([`swiss-ai/robots-txt-blocked-domains-multilingual`](https://huggingface.co/datasets/swiss-ai/robots-txt-blocked-domains-multilingual), `fw2_domains.txt`) |
| `cc_ledger` | its host or URL is in the [Common Crawl Opt-Out Ledger](https://commoncrawl.org/blog/common-crawl-foundation-opt-out-registry) (a domain entry covers its subdomains) |
| `piracy` | its host is in `lists/piracy_domains.tsv`: 725 domains and 24 labels (a label covers every registered domain whose name is the label, such as `thepiratebay.*`) from 33 sources: the EU Counterfeit and Piracy Watch List (SWD(2025) 132), the USTR 2025 Review of Notorious Markets, Dutch and Belgian blocking orders and blocking lists, and court documents on shadow libraries and torrent sites (`lists/piracy_sources.tsv`, with the page or section of each source); general platforms on those lists count too |

`host_status` records what the host's `robots.txt` fetch gave:

| `host_status` | what | effect |
|---|---|---|
| `ok` | a 2xx; the file is parsed (its first 500 KiB; content that is not robots.txt gives no rules) | its rules apply |
| `no_robots` | 404 or 410: the host has no `robots.txt` | no restriction (RFC 9309 2.3.1.3) |
| `robots_error` | a 4xx other than 404, 410 and 429 (401, 403, ...), and no 2xx, 404 or 410 on the third attempt, or no third attempt (below) | no opt-out known |
| `unreachable` | no usable answer in two attempts at least 30 minutes apart: DNS failure, no connection, TLS failure on both https and http, time-out, 429 or 5xx, more than 5 redirects, a final 1xx or 3xx, a broken response | no opt-out known |
| `no_host` | the URL has no host | no opt-out known |

**Third attempt for `robots_error`.** Attempts 1 and 2 run in Google Cloud. Many hosts answer a `403` there because
bot protection blocks the addresses of cloud providers. A host whose `robots.txt` gave `robots_error` is therefore
asked for its `robots.txt` a third time, from a server outside Google Cloud, with the same user agent, parser and
handling of 429 (`rules.attempt3_decides`):

- a 2xx makes the host `ok`, and the rules of that `robots.txt` apply (every robots reason above);
- a 404 or 410 makes it `no_robots`;
- any other 4xx, or no usable answer, leaves it at `robots_error`.

`optout_checked_at` is then the time of the fetch that decided: the third attempt when it gave a 2xx, 404 or 410,
else the attempt before it. `tdmrep.json` and `ai.txt` are not asked again; those of the earlier attempt apply.

**Third attempt stopped before its last units.** On 15 Nov 2026 the third attempt was stopped before its last units
were done. What was left were almost only subdomains of link farms and start-page clones on one slow server; at 2
hosts per registered domain at a time they would have taken hours more, for about 0.09 % of the documents in `kept/`.
`python fw2optout.py skip3` lists the hosts of the units without a result in `work/fetch3-skipped.parquet` (`unit`,
`host`, `regdom`, `n_le`). A host on that list keeps the record of attempt 1 or 2: it stays `robots_error`, with no
reservation known. Any other `robots_error` host without a record of the third attempt is an error that stops
`table`. The report gives the number of these hosts and their documents.

For a host with `robots_error`, `unreachable` or `no_host` no reservation in `robots.txt` is known; its documents are
opted out only when another reason holds (`tdmrep.json` and `ai.txt`, fetched unless the host is unreachable, and the
lists). The report counts how many documents of such hosts stay.

**Agents.** `*`, the agents in `CORE_AGENTS` in `rules.py` (among them GPTBot, ChatGPT-User, CCBot, Google-Extended,
ClaudeBot, anthropic-ai, PerplexityBot, Bytespider, Applebot-Extended, meta-externalagent, cohere-ai, Diffbot,
Omgilibot, Amazonbot), and every agent of [ai.robots.txt](https://github.com/ai-robots-txt/ai.robots.txt)
(`robots.json` at commit `795e897`, copied to `lists/`). The three robots reasons say which tier disallowed a
document, so the report shows what the wider list removes on its own (`robots_ai_extended` as the only reason).

**robots.txt matching** (`robots.py`) follows RFC 9309 and Google's open-source parser: product tokens compared
case-insensitively, groups for the same agent merged, an agent named nowhere gets `*`, longest match wins with allow
winning a tie, `*` and a final `$` in patterns, percent-normalised paths, the first 500 KiB of the file.

## Fetching

Per host, at most one request at a time: `robots.txt` over https (over http when https gives no HTTP response), then
`/.well-known/tdmrep.json` and `/ai.txt` on the scheme that answered. Redirects are followed up to 5 hops. At most 4
hosts of one registered domain at a time per process (8 for domains with more than 1,000 hosts, such as blog
platforms). The user agent is

    fineweb2-nl-optout/1.0 (+https://github.com/yhavinga/fineweb2-nl-optout; one-time check of robots.txt, tdmrep.json and ai.txt)

Hosts that failed are fetched a second time, at least 30 minutes later, with fewer requests in parallel. Hosts with
`robots_error` after that are asked for `robots.txt` a third time from a server outside Google Cloud (above), by one
process: at most 24 hosts at a time, at most 2 hosts of one registered domain at a time, each connection closed after
its response. Each host's record has the UTC time of each attempt.

Not checked: TDM reservations in HTTP headers or HTML meta tags of each page (that would be a request per document),
and reservations written in natural language in terms of use.

## Personal data

FineWeb-2 already replaces e-mail addresses and IP addresses. `pii.py` also replaces, in the text of every document
written out (`kept/` and `excluded/`): IBANs (valid length and mod-97 check), payment card numbers (scheme prefix,
length, Luhn), Dutch and Belgian phone numbers, Dutch citizen service numbers (BSN, elfproef), and Dutch postcode plus
house number. Each match becomes a fixed placeholder that is invalid by construction (`NL00 BANK 0000 0000 00`, `0000 0000 0000 0000`,
`000-0000000`, `000000000`, `0000 XX` and house number `0`); `pii_replaced` counts the replacements per document.
Names and other personal data in running text are not touched.

## Output

In the run's root (`--root`, or `$FW2OPTOUT_ROOT`, such as `gs://<bucket>/fineweb2-nl-optout`):

- `kept/{train,test}/*.parquet`: the documents up to the crawl limit with `optout` false: the training set;
- `excluded/{train,test}/*.parquet`: those with `optout` true, with the same schema; for internal use and analysis
  only, never for training or publication. A published version contains `kept/` only, and the split into two
  directories keeps it that way without a filter step;
- both with the columns of FineWeb-2 plus:

  | column | what |
  |---|---|
  | `source_file`, `source_row` | the FineWeb-2 file and the row in it |
  | `optout` | true when any reason of the rule holds |
  | `optout_reasons` | list of every reason that holds, in the order of the rule (empty when `optout` is false) |
  | `host_status` | `ok`, `no_robots`, `robots_error`, `unreachable` or `no_host` (above) |
  | `optout_checked_at` | UTC time of the fetch of the host's `robots.txt` that decided (the second attempt for a host that was unreachable at first; the third for a host with `robots_error` that then gave a 2xx, 404 or 410); null without host. The lists have one snapshot time, in `lists/manifest.json` |
  | `pii_replaced` | number of personal-data replacements in the text |

- `decisions/{train,test}/*.parquet`: for every document of FineWeb-2, also those after the crawl limit: `id`,
  `source_file`, `source_row`, `reason` (the first reason, `crawl` after the crawl limit, empty when kept) and
  `reasons` (all that hold);
- `optout/hosts.parquet`: one row per host: the time and status of each attempt (`fetched_at_1/2/3`,
  `robots_status_3`), which attempt decided (`decided_by`), the SHA-256 of its `robots.txt`, the `host_status`, the
  rules that applied, the list flags;
- `lists/`: the snapshot of the lists used, with retrieval time and SHA-256;
- `manifest.json`, `report/fineweb2-nl-optout.md` and `.json`: the counts of every step and the tokens
  (tokenizer [`yhavinga/dutch-llama-tokenizer`](https://huggingface.co/yhavinga/dutch-llama-tokenizer)).

## Files

| file | what |
|---|---|
| `rules.py` | the rule: crawl limit, agent lists, reasons and their order, the host statuses of a robots.txt fetch |
| `robots.py` | RFC 9309 parsing and matching; Content-Signal and Content-Usage |
| `decide.py` | the decision per host from what was fetched, and per document |
| `fetch.py` | the polite asynchronous fetcher |
| `lists.py` | the Apertus blocklist, the Common Crawl Opt-Out Ledger, the piracy list |
| `pii.py` | the replacement of personal data |
| `hfread.py` | reading row groups and columns of a parquet file on the Hugging Face Hub with range requests |
| `gcs.py` | bucket access and the claims that share the work between processes and VMs; the same through `gsutil` on a machine without application credentials |
| `fw2optout.py` | the phases: `plan`, `hosts`, `shards`, `lists`, `fetch`, `hosts3`, `skip3`, `table`, `filter`, `report` |
| `report.py` | the report and the manifest of a finished run |
| `run_phase.sh` | runs one phase in several low-priority processes on one machine |
| `lists/` | the agent list of ai.robots.txt at a fixed commit; the piracy list (`piracy_domains.tsv`) and its sources (`piracy_sources.tsv`) |
| `tests/` | the tests of the rule, the parser, the personal-data patterns, the reader and the fetcher |

## Running

Python 3.10 or later, `pip install -r requirements.txt`. Each phase claims units of work in the bucket (an object
created only if it does not exist), so it can run in any number of processes on any number of machines; a claim
without a result after two hours is taken over. Every command works under one root in a bucket:

    export FW2OPTOUT_ROOT=gs://<bucket>/fineweb2-nl-optout
    python fw2optout.py plan
    bash run_phase.sh hosts 8
    python fw2optout.py shards
    bash run_phase.sh fetch1 8 && bash run_phase.sh fetch2 8
    python fw2optout.py hosts3
    # on a server outside Google Cloud; --gsutil when it has no application default credentials
    nice -n 19 python fw2optout.py --gsutil fetch --attempt 3 --concurrency 24 --per-domain 2 --parallel 6
    # only when attempt 3 is stopped before every unit is done, after its processes are stopped
    python fw2optout.py skip3
    python fw2optout.py lists
    python fw2optout.py table
    bash run_phase.sh filter 32 --run <run>
    python fw2optout.py report --out report --commit <commit> --run <run>

## Tests

    pip install pytest ruff
    python -m pytest -q tests
    ruff check . && ruff format --check .

## Acknowledgements

Research supported with Cloud TPUs from Google's [TPU Research Cloud (TRC)](https://sites.research.google/trc/about/).

## Licences

- **Code**: Apache License 2.0 (`LICENSE`).
- **FineWeb-2**: ODC-By 1.0; its use is also subject to Common Crawl's Terms of Use.
- **ai.robots.txt** (`lists/ai-robots-txt-795e897.json`): MIT License.
