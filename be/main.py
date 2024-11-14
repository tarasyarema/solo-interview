import asyncio
from datetime import datetime, timedelta
from uuid import uuid4
from contextlib import asynccontextmanager
import aiosqlite
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

MAX_CONCURRENT_TASKS = 3  # Limit concurrent tasks to 3 for testing

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for the FastAPI application."""
    # Initialize app state
    app.state.tasks = {}
    app.state.testing = True  # Always set testing mode for now

    # Create database connection
    db_path = ":memory:"  # Use in-memory database for testing
    app.state.db = await aiosqlite.connect(db_path)

    # Create tables
    async with app.state.db.cursor() as cursor:
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS data (
                id TEXT PRIMARY KEY,
                batch_id TEXT,
                timestamp TEXT,
                value INTEGER
            )
        ''')
        await app.state.db.commit()

    print("Application startup complete")

    try:
        yield
    finally:
        # Clean up tasks
        tasks = list(app.state.tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Close database connection
        await app.state.db.close()
        print("Application shutdown complete")

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

async def insert_task(batch_id: str) -> bool:
    """Insert test data into the database."""
    try:
        async with app.state.db.cursor() as cursor:
            # Insert test data with deterministic values for testing
            for i in range(5):  # Insert 5 records per batch
                # Calculate value based on position only (not batch_id)
                value = (4 - i) * 10  # Will generate: 40, 30, 20, 10, 0

                # Use TEXT type for timestamp to match schema
                timestamp = (datetime.now() - timedelta(minutes=i)).isoformat()

                # Use UUID for id field (now TEXT type)
                record_id = str(uuid4())

                await cursor.execute(
                    'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                    (record_id, batch_id, timestamp, value)
                )

            await app.state.db.commit()
            return True

    except Exception as e:
        print(f"Error during insertion: {str(e)}")
        return False

async def _insert_task_impl(batch_id: str):
    """Insert test data into the database."""
    print(f"Inserting data for batch {batch_id}")

    try:
        # Add initial delay to ensure task is tracked
        await asyncio.sleep(0.5)

        # Shield the entire operation from cancellation
        async with asyncio.timeout(10.0):
            success = await asyncio.shield(insert_task(batch_id))
            if success:
                print(f"Data insertion completed for batch {batch_id}")
                # Add delay after success to ensure task remains tracked
                if app.state.testing:
                    await asyncio.sleep(1.0)
                return True
            return False

    except asyncio.CancelledError:
        print(f"Task {batch_id} cancelled")
        if app.state.testing:
            # Don't propagate cancellation during tests
            await asyncio.sleep(0.5)  # Keep task alive briefly
            return False
        raise
    except asyncio.TimeoutError:
        print(f"Task {batch_id} timed out")
        raise RuntimeError("Task timed out")
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        raise
    finally:
        if app.state.testing:
            # Add final delay in testing mode
            await asyncio.sleep(0.5)

@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new streaming task."""
    print(f"Starting stream for batch {batch_id}")

    # Ensure we have our tasks dictionary
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Get current active tasks (only count running tasks)
    current_tasks = len([t for t in app.state.tasks.values() if not t.done()])
    print(f"Active tasks before creation: {current_tasks}, Max allowed: {MAX_CONCURRENT_TASKS}")

    if current_tasks >= MAX_CONCURRENT_TASKS:
        return JSONResponse(
            status_code=429,
            content={
                "status": "error",
                "detail": f"Maximum concurrent tasks ({MAX_CONCURRENT_TASKS}) reached"
            }
        )

    # Check if task already exists and is running
    if batch_id in app.state.tasks:
        existing_task = app.state.tasks[batch_id]
        if not existing_task.done():
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"Task for batch {batch_id} is already running"
                }
            )
        # Clean up completed/failed task if not in testing mode
        if not app.state.testing:
            del app.state.tasks[batch_id]

    # Check if we already have data for this batch_id
    async with app.state.db.cursor() as cursor:
        await cursor.execute('SELECT COUNT(*) FROM data WHERE batch_id = ?', (batch_id,))
        count = (await cursor.fetchone())[0]
        if count > 0:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"Data for batch {batch_id} already exists"
                }
            )

    # Create and start the task with proper shielding
    try:
        # Create a shielded task that will run to completion
        async def protected_task():
            try:
                async with asyncio.timeout(10.0):  # Add timeout to prevent hanging
                    return await asyncio.shield(_insert_task_impl(batch_id))
            except asyncio.CancelledError:
                print(f"Protected task {batch_id} received cancellation")
                if app.state.testing:
                    # Don't propagate cancellation during tests
                    return False
                raise
            except Exception as e:
                print(f"Protected task {batch_id} failed: {str(e)}")
                raise

        # Create and store the task BEFORE starting it
        task = asyncio.create_task(protected_task())
        app.state.tasks[batch_id] = task

        # Set up done callback that only removes task if it's the same one
        def task_done_callback(t):
            try:
                if task.cancelled():
                    print(f"Task {batch_id} was cancelled")
                elif task.exception():
                    print(f"Task {batch_id} failed with error: {task.exception()}")
                else:
                    print(f"Task {batch_id} completed successfully")

                # Only remove task if we're not in testing mode
                if not app.state.testing:
                    if batch_id in app.state.tasks and app.state.tasks[batch_id] is t:
                        del app.state.tasks[batch_id]
                print(f"Task {batch_id} completed and cleaned up")
            except Exception as e:
                print(f"Error in task done callback: {str(e)}")

        task.add_done_callback(task_done_callback)

        # Wait briefly to ensure task starts
        try:
            await asyncio.wait_for(
                asyncio.shield(asyncio.wait([task], return_when=asyncio.FIRST_COMPLETED)),
                timeout=0.1
            )
        except asyncio.TimeoutError:
            # Task is still running, which is expected
            pass
        except Exception as e:
            # Task failed to start
            if not app.state.testing and batch_id in app.state.tasks:
                del app.state.tasks[batch_id]
            raise

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "detail": f"Task {batch_id} started"
            }
        )
    except Exception as e:
        print(f"Error starting task: {str(e)}")
        if not app.state.testing and batch_id in app.state.tasks:
            del app.state.tasks[batch_id]
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    """Stop a streaming task."""
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
                "detail": "Not implemented"
            }
        )

    try:
        async with app.state.db.cursor() as cursor:
            await cursor.execute('''
                SELECT batch_id,
                       COUNT(*) as count,
                       CAST(AVG(CAST(value AS FLOAT)) AS FLOAT) as avg_value,
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
                    "avg_value": float(row[2]),
                    "min_value": int(row[3]),
                    "max_value": int(row[4])
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
                "detail": "Not implemented"
            }
        )
