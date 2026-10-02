# Chain-Mind Auditor
A backend script that listens to real pending transactions on the Ethereum
Sepolia testnet mempool, filters out routine activity, and uses a locally
running LLM (Ollama) to flag anomalous transactions in plain English —
with no external AI API involved.

## Why this approach
The assignment allowed two data sources (mempool or verified contract
source) and two processing methods (rule-based pattern matching or an
LLM). I chose the (mempool + LLM) combination because it works on live,
real-time data rather than a one-off lookup, which felt closer to a real
monitoring tool.

The bonus objective asks for the LLM to run fully on-device instead of
relying on an external API — I used **Ollama** running the **phi3**
model locally for this reason. Note that fetching mempool data itself
still requires an external connection (Alchemy) since pending
transactions don't exist anywhere except inside network-connected nodes
— this is a data-access requirement, not a processing dependency, so it
doesn't conflict with the "local LLM" goal.

Pure LLM-per-transaction doesn't scale against a live mempool, since
transactions can arrive faster than a model can analyze them. To solve
this, pattern-matching style filtering (the other allowed option) is used
**as a pre-filter stage** — cheap, fast rules decide which transactions
are worth sending to the LLM at all, instead of overwhelming it with
routine traffic. This hybrid approach was a deliberate design choice, not
a shortcut.

## How it works
1. Connects to Alchemy's Sepolia WebSocket and subscribes to pending
   transactions.
2. Each incoming transaction hash is looked up over a separate HTTPS
   call (kept separate from the WebSocket to avoid both trying to read
   from the same connection at once, which caused real conflicts during
   testing).
3. A pre-filter drops low-value, routine transactions, keeping only
   ones above a value threshold or using an unrecognized function.
4. Surviving transactions are decoded into readable fields (function
   name, ETH value, gas price in Gwei) and tagged with how many times
   that sender has shown up this session.
5. Every few seconds, a batch of transactions is sent to the local
   Ollama model, which returns a verdict (ANOMALOUS / NORMAL) and a
   reason for each.
6. Results print to the terminal; a status line reports queue health
   every 15 seconds.

## Setup
python -m venv venv
venv\Scripts\activate        (Windows)
pip install -r requirements.txt
ollama pull phi3
ollama serve
Copy `.env.example` to `.env` and fill in your own Alchemy Sepolia
WebSocket URL, then run:
python auditor.py

## Files
- `auditor.py` — the backend script (ingestion + processing)
- `requirements.txt` — Python dependencies
- `.env.example` — template for required environment variables
- `screenshots/` — terminal output showing the pipeline running