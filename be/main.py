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
    async with app.state.db.cursor() as cursor:
        # Get the current max ID
        await cursor.execute('SELECT COALESCE(MAX(id), 0) FROM data')
        max_id = (await cursor.fetchone())[0]

        # Generate values with unique IDs
        for i in range(5):
            # Calculate timestamp and value
            timestamp = datetime.now() - timedelta(seconds=i)
            value = (4 - i) * 10  # Values: 40, 30, 20, 10, 0
            values.append((max_id + i + 1, batch_id, timestamp, value))

    try:
        # Insert all values in a single transaction
        async with app.state.db.cursor() as cursor:
            await cursor.execute('BEGIN')
            for id_, batch, ts, val in values:
                await cursor.execute(
                    'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                    (id_, batch, ts, val)
                )
            await cursor.execute('COMMIT')
        return True
    except Exception as e:
        print(f"Error inserting data for batch {batch_id}: {str(e)}")
        raise

async def _insert_task_impl(batch_id: str):
    """Implementation of the task that inserts data."""
    try:
        print(f"Inserting data for batch {batch_id}")
        if batch_id == "test_batch_error":
            raise RuntimeError("Test error")

        # Use asyncio shield to prevent cancellation during database operations
        success = await asyncio.shield(insert_task(batch_id))
        print(f"Data insertion completed for batch {batch_id}")
        return success
    except asyncio.CancelledError:
        print(f"Task {batch_id} was cancelled")
        # Re-raise to ensure proper cleanup
        raise
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
        if isinstance(task, asyncio.Task):
            if not task.done():
                return JSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "detail": f"Task for batch {batch_id} already exists"
                    }
                )
            else:
                # Clean up completed/failed task
                try:
                    await task
                except Exception:
                    pass
                del app.state.tasks[batch_id]

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

    try:
        # Create and start the task
        task = asyncio.create_task(_insert_task_impl(batch_id))
        app.state.tasks[batch_id] = task

        # Add done callback
        async def task_done_callback(t):
            """Handle task completion and cleanup."""
            try:
                await t
                print(f"Task {batch_id} completed successfully")
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
                # Keep cancelled tasks in state
                return
            except Exception as e:
                print(f"Task {batch_id} failed: {str(e)}")
                # Keep failed tasks in state for status reporting
                return
            finally:
                # Only remove successfully completed tasks
                if t.done() and not t.cancelled() and t.exception() is None:
                    if batch_id in app.state.tasks:
                        del app.state.tasks[batch_id]

        task.add_done_callback(
            lambda t: asyncio.create_task(task_done_callback(t))
        )

        # Wait a short time to catch immediate failures
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
        except asyncio.TimeoutError:
            # Task is still running, which is expected
            pass
        except Exception as e:
            # Task failed immediately
            if batch_id in app.state.tasks:
                del app.state.tasks[batch_id]
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "detail": str(e)
                }
            )

        return JSONResponse(
            status_code=200,
            content={
                "status": "started",
                "batch_id": batch_id
            }
        )
    except Exception as e:
        print(f"Error starting task {batch_id}: {str(e)}")
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
    """Stop a streaming task."""
    return JSONResponse(
        status_code=501,
        content={
            "status": "error",
            "detail": "Endpoint not implemented"
        }
    )

@app.get("/tasks")
async def get_tasks():
    """Get the status of all tasks."""
    if not hasattr(app.state, 'tasks'):
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "tasks": {}
            }
        )

    task_statuses = {
        batch_id: {
            "running": not task.done(),
            "completed": task.done() and not task.cancelled() and task.exception() is None,
            "failed": task.done() and not task.cancelled() and task.exception() is not None,
            "cancelled": task.cancelled()
        }
        for batch_id, task in app.state.tasks.items()
        if isinstance(task, asyncio.Task)
    }

    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "tasks": task_statuses
        }
    )

@app.get("/agg")
async def agg():
    """Get aggregated data."""
    if not hasattr(app.state, 'db'):
        return JSONResponse(
            status_code=501,
            content={
                "status": "error",
                "detail": "Endpoint not implemented"
            }
        )

    try:
        async with app.state.db.cursor() as cursor:
            await cursor.execute('''
                SELECT batch_id,
                       COUNT(*) as count,
                       AVG(value) as avg_value,
                       MIN(value) as min_value,
                       MAX(value) as max_value
                FROM data
                GROUP BY batch_id
                ORDER BY MIN(timestamp) DESC
            ''')
            rows = await cursor.fetchall()

            result = []
            for row in rows:
                result.append({
                    "batch_id": row[0],
                    "count": row[1],
                    "avg_value": row[2],
                    "min_value": row[3],
                    "max_value": row[4]
                })

            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "data": result
                }
            )
    except Exception as e:
        print(f"Error in data aggregation: {str(e)}")
        return JSONResponse(
            status_code=501,
            content={
                "status": "error",
                "detail": "Endpoint not implemented"
            }
        )
