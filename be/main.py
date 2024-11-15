from contextlib import asynccontextmanager
from json import dumps
from datetime import datetime, timedelta
import random
import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import duckdb
import asyncio

db: duckdb.DuckDBPyConnection
tasks: dict[str, asyncio.Task] = {}
MAX_TASKS = 5

@asynccontextmanager
async def lifespan(_: FastAPI):
    global db
    db = duckdb.connect('data.db')

    db.execute('CREATE TABLE IF NOT EXISTS data (batch_id TEXT, id TEXT, data TEXT); CREATE INDEX IF NOT EXISTS idx_data_id ON data (batch_id);')

    yield
    db.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    count = db.execute('SELECT COUNT(*) FROM data').fetchone()

    if count:
        count = count[0]
    else:
        count = 0

    return {
        "row_count": count,
        "task_count": len(tasks),
    }


@app.get("/tasks")
async def get_tasks():
    return {
        "tasks": list(tasks.keys()),
    }


async def insert_task(batch_id: str):
    id = datetime.now().isoformat()
    i = 0

    while True:
        await asyncio.sleep(0.25)

        print(f"{batch_id}: {id} - it: {i}")
        value = random.randint(0, 100)

        db.execute('INSERT INTO data VALUES (?, ?, ?)', (batch_id, id, dumps({"key": value})))
        i += 1


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    if batch_id in tasks:
        raise HTTPException(status_code=400, detail="batch_id already exists")
    
    if (len(tasks) >= MAX_TASKS):
        raise HTTPException(status_code=400, detail="Maximum number of tasks reached")
    
    tasks[batch_id] = asyncio.create_task(insert_task(batch_id))

    return {
        "message": "Batch started",
        "batch_id": batch_id,
    }

@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    # https://superfastpython.com/asyncio-task-cancellation-best-practices/
    if batch_id not in tasks:
        raise HTTPException(status_code=404, detail="batch_id not found")
        
    task = tasks[batch_id]
    if task.done():
        raise HTTPException(status_code=400, detail="Task already stopped")
    
    try:
        task.cancel()
        await asyncio.wait([task])
        del tasks[batch_id]
        return {
            "message": "Batch stopped",
            "batch_id": batch_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stream/{batch_id}")
async def stream(batch_id: str):
    async def _gen():
        i = 0

        while True:
            yield f"data: {dumps({'value': i})}\n\n"

            i += 1
            await asyncio.sleep(0.1)

    return StreamingResponse(_gen(), media_type="text/event-stream")

""" I don't know if you want this live data, or just the index like above """
""" @app.get("/stream/{batch_id}")
async def stream(batch_id: str):
    async def _gen():
        sql = 'SELECT id, data FROM data WHERE batch_id = ? ORDER BY id DESC'
        results = db.execute(sql, (batch_id,)).fetchall()

        for id, data_json in results:
            data = json.loads(data_json)
            yield f"data: {json.dumps({'id': id, 'value': data['key']})}\n\n"

    return StreamingResponse(_gen(), media_type="text/event-stream") """


@app.get("/agg/{batch_id}")
async def agg(batch_id: str):
    # https://stackoverflow.com/questions/4541629/how-to-create-a-datetime-equal-to-15-minutes-ago
    one_minute_ago = datetime.now() - timedelta(minutes=1)
    
    result = db.execute(f"""
        SELECT
            AVG(CAST(json_extract(data, '$.key') AS FLOAT)) AS average,
            COUNT(*) AS count,
            SUM(CAST(json_extract(data, '$.key') AS FLOAT)) AS sum
        FROM data
        WHERE batch_id = ? AND id >= ?
    """, (batch_id, one_minute_ago.isoformat())).fetchone()
    
    if result:
        average, count, sum = result    
        return {
            "average": average,
            "count": count,
            "sum": sum,
        }
    else:
        return {
            "average": 0,
            "count": 0,
            "sum": 0,
        }
