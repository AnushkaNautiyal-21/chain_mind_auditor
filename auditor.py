import asyncio
import json
import os
from collections import deque
import requests
import websockets

from dotenv import load_dotenv
load_dotenv()

ws_url=os.getenv("ALCHEMY_WS_URL")
http_url=ws_url.replace("wss://","https://") if ws_url else None
ollama_url=os.getenv("OLLAMA_URL","http://localhost:11434/api/generate")
model=os.getenv("OLLAMA_MODEL","phi3")
watch_contract=os.getenv("WATCH_CONTRACT")
min_value=0.3
queue_limit=40
batch_size=5
batch_time=4
status_time=15
selectors={
    "0xa9059cbb":"transfer(address,uint256)",
    "0x095ea7b3":"approve(address,uint256)",
    "0x23b872dd":"transferFrom(address,address,uint256)",
    "0x2e1a7d4d":"withdraw(uint256)",
    "0xd0e30db0":"deposit()"
}

tokens={
    "0x1c7d4b196cb0c7b01d743fbc6116a902379c7238":"USDC"
}

tx_queue=deque()
processed=0
filtered=0
sender_count={}
def to_int(value):
    if value is None:
        return 0
    if isinstance(value,int):
        return value
    return int(value,16) if value.startswith("0x") else int(value)
def to_eth(value):
    return value/10**18
def to_gwei(value):
    return value/10**9
async def main():
    if not ws_url:
        print("[ERROR] Alchemy WebSocket URL not found")
        return
    if not check_ollama():
        print("[ERROR] Ollama is not running")
        return
    print("[INFO] Connecting to Alchemy...")
    async with websockets.connect(ws_url) as ws:
        request={"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}
        await ws.send(json.dumps(request))
        await ws.recv()
        if watch_contract:
            print("[INFO] Watching:",watch_contract)
        else:
            print("[INFO] Watching all pending transactions")
        asyncio.create_task(batch_loop())
        asyncio.create_task(status_loop())
        async for message in ws:
            data=json.loads(message)
            tx_hash=data.get("params",{}).get("result")
            if tx_hash:
                asyncio.create_task(get_transaction(tx_hash))

def get_transaction_http(tx_hash):
    request={"jsonrpc":"2.0","id":2,"method":"eth_getTransactionByHash","params":[tx_hash]}
    response=requests.post(http_url,json=request,timeout=10)
    response.raise_for_status()
    return response.json().get("result")

async def get_transaction(tx_hash):
    try:
        tx=await asyncio.to_thread(get_transaction_http,tx_hash)
        if tx:
            process_transaction(tx)
    except Exception as e:
        print("[WARN] Could not get transaction:",e)

def process_transaction(tx):
    global filtered
    value=to_eth(to_int(tx.get("value","0x0")))
    input_data=tx.get("input","0x")
    selector=input_data[:10] if input_data else "0x"
    receiver=(tx.get("to") or "").lower()
    if watch_contract and receiver!=watch_contract.lower():
        filtered+=1
        return
    if value<min_value and selector in selectors:
        filtered+=1
        return
    sender=tx.get("from","unknown")
    sender_count[sender]=sender_count.get(sender,0)+1
    if len(tx_queue)>=queue_limit:
        tx_queue.popleft()
    tx_queue.append(tx)
    print(f"[QUEUED] {tx.get('hash','')[:14]}... value={value:.3f} ETH")

def decode_transaction(tx):
    input_data=tx.get("input","0x")
    selector=input_data[:10] if input_data else "0x"
    receiver=(tx.get("to") or "").lower()
    sender=tx.get("from","unknown")
    value=to_eth(to_int(tx.get("value","0x0")))
    gas_price=to_gwei(to_int(tx.get("gasPrice","0x0")))
    return {
        "hash":tx.get("hash"),
        "from":sender,
        "to":tx.get("to"),
        "function":selectors.get(selector,f"unknown({selector})"),
        "token":tokens.get(receiver,"ETH"),
        "value_eth":round(value,5),
        "gas_price_gwei":round(gas_price,2),
        "sender_tx_count":sender_count.get(sender,1)
    }

async def batch_loop():
    while True:
        await asyncio.sleep(batch_time)
        await send_batch()

async def send_batch():
    global processed
    if not tx_queue:
        return
    transactions=[tx_queue.popleft() for _ in range(min(batch_size,len(tx_queue)))]
    decoded=[decode_transaction(tx) for tx in transactions]
    prompt=make_prompt(decoded)
    try:
        answer=await asyncio.to_thread(call_ollama,prompt)
    except Exception as e:
        print("[ERROR] Ollama request failed:",e)
        return
    results=parse_answer(answer,decoded)
    print("\n================ RESULTS ================\n")
    for result in results:
        print("TX:",result.get("hash","")[:14]+"...")
        print("Verdict:",result.get("verdict"))
        print("Reason:",result.get("reason"))
        print()
    processed+=len(decoded)

def make_prompt(transactions):
    return (
        "You are checking Ethereum pending transactions.\n"
        "Look at each transaction and classify it as NORMAL or ANOMALOUS.\n"
        "Consider gas price, unknown functions, repeated senders and unusual transaction values.\n"
        "Return only a JSON array in this format:\n"
        '[{"hash":"...","verdict":"NORMAL","reason":"..."}]\n\n'
        "Transactions:\n"+json.dumps(transactions,indent=2)
    )
def call_ollama(prompt):
    response=requests.post(
        ollama_url,
        json={"model":model,"prompt":prompt,"stream":False},
        timeout=60
    )
    response.raise_for_status()
    return response.json()["response"]
def check_ollama():
    try:
        call_ollama("Reply with OK")
        return True
    except Exception:
        return False
def parse_answer(answer,transactions):
    try:
        answer=answer.replace("```json","").replace("```","").strip()
        return json.loads(answer)
    except Exception:
        return [
            {"hash":tx["hash"],"verdict":"REVIEW","reason":"Could not understand Ollama response"}
            for tx in transactions
        ]
async def status_loop():
    while True:
        await asyncio.sleep(status_time)
        print(f"[STATUS] queue={len(tx_queue)} processed={processed} filtered={filtered}")
if __name__=="__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
