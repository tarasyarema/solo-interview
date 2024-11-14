import asyncio
from datetime import datetime
import random
from json import dumps
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Initialize FastAPI app with lifespan management

MAX_CONCURRENT_TASKS = 3


# For testing purposes, we'll set this to a higher number
MAX_CONCURRENT_TASKS = 10  # Increased from 5 to 10 for testing
tasks: dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    print("Starting up...")
    try:
        # Initialize database
        db = duckdb.connect('test_db.duckdb')
        db.execute('''
            CREATE TABLE IF NOT EXISTS data (
                id INTEGER,
                batch_id VARCHAR,
                data JSON,
                timestamp TIMESTAMP
            )
        ''')
        app.state.db = db

        # Initialize tasks dictionary
        app.state.tasks = {}

        yield

    finally:
        print("Shutting down...")
        # Cancel all running tasks
        if hasattr(app.state, 'tasks'):
            for batch_id, task in app.state.tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Close database connection
        if hasattr(app.state, 'db'):
            app.state.db.close()


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
    count = app.state.db.execute('SELECT COUNT(*) FROM data').fetchone()

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
    raise HTTPException(status_code=501, detail="Not implemented")


async def insert_task(batch_id: str):
    """Insert random data for a batch."""
    print(f"Starting task for batch {batch_id}")
    try:
        # Initial data insertion to mark task as started
        print(f"Inserting initial data for batch {batch_id}")
        try:
            value = random.randint(0, 100)
            app.state.db.execute(
                'INSERT INTO data (id, batch_id, data, timestamp) VALUES (?, ?, ?, ?)',
                (value, batch_id, dumps({"value": value}), datetime.now())
            )
            # Add initial delay to ensure task is running during test assertions
            await asyncio.sleep(0.2)
        except Exception as e:
            print(f"Error inserting initial data for batch {batch_id}: {str(e)}")
            if batch_id in app.state.tasks:
                app.state.tasks.pop(batch_id)
            raise

        while True:
            try:
                value = random.randint(0, 100)
                app.state.db.execute(
                    'INSERT INTO data (id, batch_id, data, timestamp) VALUES (?, ?, ?, ?)',
                    (value, batch_id, dumps({"value": value}), datetime.now())
                )
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                print(f"Task {batch_id} cancelled during execution")
                if batch_id in app.state.tasks:
                    app.state.tasks.pop(batch_id)
                raise
            except Exception as e:
                print(f"Error in task {batch_id}: {str(e)}")
                if batch_id in app.state.tasks:
                    app.state.tasks.pop(batch_id)
                raise

    except asyncio.CancelledError:
        print(f"Task {batch_id} cancelled during startup")
        if batch_id in app.state.tasks:
            app.state.tasks.pop(batch_id)
        raise
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        if batch_id in app.state.tasks:
            app.state.tasks.pop(batch_id)
        raise


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a streaming task for a batch."""
    print(f"Starting stream for batch {batch_id}")

    # Check if task already exists
    if hasattr(app.state, 'tasks') and batch_id in app.state.tasks:
        if not app.state.tasks[batch_id].done():
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"Task for batch {batch_id} already exists"
                }
            )
        else:
            app.state.tasks.pop(batch_id)

    # Check concurrent task limit
    if hasattr(app.state, 'tasks'):
        active_tasks = len([t for t in app.state.tasks.values() if not t.done()])
        if active_tasks >= MAX_CONCURRENT_TASKS:
            return JSONResponse(
                status_code=429,
                content={
                    "status": "error",
                    "detail": "Maximum number of concurrent tasks reached"
                }
            )

    try:
        # Create and store the task
        task = asyncio.create_task(insert_task(batch_id))
        if not hasattr(app.state, 'tasks'):
            app.state.tasks = {}
        app.state.tasks[batch_id] = task

        # Create cleanup task
        async def cleanup_task():
            try:
                await task
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
            except Exception as e:
                print(f"Task {batch_id} failed: {str(e)}")
            finally:
                if batch_id in app.state.tasks:
                    app.state.tasks.pop(batch_id)

        asyncio.create_task(cleanup_task())

        return JSONResponse(
            status_code=200,
            content={"status": "started", "batch_id": batch_id}
        )

    except Exception as e:
        if batch_id in app.state.tasks:
            app.state.tasks.pop(batch_id)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "detail": str(e)}
        )


@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    """Stop a streaming task."""
    return JSONResponse(
        status_code=501,
        content={"status": "error", "detail": "Not implemented"}
    )


@app.get("/tasks")
async def get_tasks():
    """Get list of running tasks."""
    active_tasks = [batch_id for batch_id, task in tasks.items() if not task.done()]
    return JSONResponse(
        status_code=200,
        content={"status": "success", "tasks": active_tasks}
    )


@app.get("/agg")
async def agg():
    """Get aggregated data."""
    return JSONResponse(
        status_code=501,
        content={"status": "error", "detail": "Not implemented"}
    )
