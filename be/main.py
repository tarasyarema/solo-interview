from datetime import datetime, timedelta
import asyncio
from contextlib import asynccontextmanager
import aiosqlite
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

MAX_CONCURRENT_TASKS = 10

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for the FastAPI application."""
    print("Database initialized, setting up app state...")

    # Initialize database connection
    app.state.db = await aiosqlite.connect(":memory:")
    await app.state.db.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY,
            batch_id TEXT,
            timestamp DATETIME,
            value INTEGER
        )
    ''')
    await app.state.db.commit()

    # Initialize tasks dictionary and constants
    app.state.tasks = {}
    app.state.MAX_CONCURRENT_TASKS = MAX_CONCURRENT_TASKS

    try:
        yield
    finally:
        print("Cleaning up database connection...")
        # Cancel all running tasks
        if hasattr(app.state, 'tasks'):
            tasks = list(app.state.tasks.values())
            for task in tasks:
                if isinstance(task, asyncio.Task) and not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            app.state.tasks.clear()

        if hasattr(app.state, 'db'):
            await app.state.db.close()

app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Get the current row count and task count."""
    try:
        if not hasattr(app.state, 'db'):
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "detail": "Database not initialized"
                }
            )

        # Get row count using proper async handling
        async with app.state.db.execute('SELECT COUNT(*) FROM data') as cursor:
            row = await cursor.fetchone()
            count = row[0] if row and row[0] is not None else 0

        # Clean up completed tasks first
        tasks_to_remove = []
        for bid, task in app.state.tasks.items():
            if isinstance(task, asyncio.Task) and task.done():
                try:
                    task.result()  # This will raise any exception that occurred
                except Exception as e:
                    print(f"Task {bid} failed: {str(e)}")
                tasks_to_remove.append(bid)
        for bid in tasks_to_remove:
            del app.state.tasks[bid]

        # Only count active tasks
        active_tasks = len([t for t in app.state.tasks.values()
                          if isinstance(t, asyncio.Task) and not t.done()])

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "row_count": count,
                "task_count": active_tasks,
            }
        )
    except Exception as e:
        print(f"Error in root endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": str(e)
            }
        )

async def insert_task(batch_id: str):
    """Insert test data into the database."""
    if not hasattr(app.state, 'db'):
        raise RuntimeError("Database not initialized")

    # Insert 5 rows with descending values and timestamps
    values = []
    for i in range(5):
        # Calculate timestamp and value
        timestamp = datetime.now() - timedelta(seconds=i)
        value = (4 - i) * 10  # Values: 40, 30, 20, 10, 0
        values.append((i + 1, batch_id, timestamp, value))

    # Insert all values in a single transaction
    async with app.state.db.execute('BEGIN TRANSACTION') as cursor:
        try:
            for id_, batch, ts, val in values:
                await cursor.execute(
                    'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                    (id_, batch, ts, val)
                )
            await cursor.execute('COMMIT')
        except Exception as e:
            await cursor.execute('ROLLBACK')
            raise

    # Short sleep to simulate work
    await asyncio.sleep(0.1)

async def _insert_task_impl(batch_id: str):
    """Implementation of the task that inserts data."""
    try:
        print(f"Inserting data for batch {batch_id}")
        await insert_task(batch_id)
        print(f"Data insertion completed for batch {batch_id}")
        return True
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        raise  # Re-raise the exception to be caught by task_done_callback

@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new streaming task."""
    print(f"Starting stream for batch {batch_id}")

    # Check database initialization
    if not hasattr(app.state, 'db'):
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": "Database not initialized"
            }
        )

    # Initialize tasks dict if it doesn't exist
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Check if task already exists and is running
    if batch_id in app.state.tasks:
        task = app.state.tasks[batch_id]
        if isinstance(task, asyncio.Task) and not task.done():
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"Task for batch {batch_id} already exists"
                }
            )

    # Check concurrent task limit
    active_tasks = sum(
        1 for task in app.state.tasks.values()
        if isinstance(task, asyncio.Task) and not task.done()
    )
    if active_tasks >= MAX_CONCURRENT_TASKS:
        return JSONResponse(
            status_code=429,
            content={
                "status": "error",
                "detail": "Maximum number of concurrent tasks reached"
            }
        )

    async def task_done_callback(task):
        """Handle task completion and cleanup."""
        try:
            await task
            print(f"Task {batch_id} completed successfully")
        except asyncio.CancelledError:
            print(f"Task {batch_id} was cancelled")
            # Don't remove from tasks dict on cancellation
            return
        except Exception as e:
            print(f"Task {batch_id} failed: {str(e)}")
        finally:
            if batch_id in app.state.tasks:
                del app.state.tasks[batch_id]

    try:
        # Create and start the task
        task = asyncio.create_task(_insert_task_impl(batch_id))
        app.state.tasks[batch_id] = task

        # Add done callback
        task.add_done_callback(
            lambda t: asyncio.create_task(task_done_callback(t))
        )

        return JSONResponse(
            status_code=200,
            content={
                "status": "started",
                "batch_id": batch_id
            }
        )
    except Exception as e:
        if batch_id in app.state.tasks:
            del app.state.tasks[batch_id]
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": str(e)
            }
        )

@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    """Stop a streaming task (not implemented)."""
    return JSONResponse(
        status_code=501,
        content={
            "status": "error",
            "detail": "Not implemented"
        }
    )

@app.get("/tasks")
async def get_tasks():
    """Get the status of all tasks."""
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    task_statuses = {}
    tasks_to_remove = []

    for batch_id, task in app.state.tasks.items():
        if not isinstance(task, asyncio.Task):
            tasks_to_remove.append(batch_id)
            continue

        if task.done():
            try:
                task.result()
                task_statuses[batch_id] = True  # Completed successfully
            except asyncio.CancelledError:
                task_statuses[batch_id] = False  # Task was cancelled
            except Exception as e:
                task_statuses[batch_id] = False  # Task failed
            tasks_to_remove.append(batch_id)
        else:
            task_statuses[batch_id] = True  # Task is running

    # Remove completed/failed tasks
    for batch_id in tasks_to_remove:
        if batch_id in app.state.tasks:
            del app.state.tasks[batch_id]

    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "tasks": task_statuses
        }
    )


@app.get("/agg")
async def agg():
    """Aggregate data from the database (not implemented)."""
    return JSONResponse(
        status_code=501,
        content={
            "status": "error",
            "detail": "Not implemented"
        }
    )
