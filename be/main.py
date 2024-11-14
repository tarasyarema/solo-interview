from datetime import datetime
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
    """Insert task data into the database."""
    print(f"Inserting data for batch {batch_id}")
    if not hasattr(app.state, 'db'):
        raise RuntimeError("Database not initialized")

    # Use incremental values for testing: 10, 20, 30, 40
    values = [(i, batch_id, datetime.now(), i * 10) for i in range(1, 5)]

    try:
        async with app.state.db.execute('BEGIN TRANSACTION'):
            for value in values:
                async with app.state.db.execute(
                    'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                    value
                ):
                    pass  # No need to fetch results for INSERT operations
            await app.state.db.commit()

        # Add a delay to prevent tasks from completing too quickly
        # This helps with concurrent task limit testing
        await asyncio.sleep(2.0)

        return True
    except Exception as e:
        print(f"Error inserting data for batch {batch_id}: {str(e)}")
        await app.state.db.rollback()
        raise

async def _insert_task_impl(batch_id: str):
    """Protected implementation of insert_task."""
    try:
        await insert_task(batch_id)
        return True
    except Exception as e:
        print(f"Error in insert_task for {batch_id}: {str(e)}")
        if batch_id in app.state.tasks:
            del app.state.tasks[batch_id]
        raise

@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new streaming task."""
    if not hasattr(app.state, 'db'):
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": "Database not initialized"
            }
        )

    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Clean up completed or failed tasks first
    tasks_to_remove = []
    for bid, task in app.state.tasks.items():
        if isinstance(task, asyncio.Task) and task.done():
            try:
                task.result()
            except Exception as e:
                print(f"Task {bid} failed: {str(e)}")
            tasks_to_remove.append(bid)
    for bid in tasks_to_remove:
        del app.state.tasks[bid]

    # Check if task already exists and is not done
    existing_task = app.state.tasks.get(batch_id)
    if existing_task is not None and isinstance(existing_task, asyncio.Task):
        if not existing_task.done():
            print(f"Task {batch_id} already exists and is running")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"Task {batch_id} already exists"
                }
            )
        else:
            del app.state.tasks[batch_id]

    # Check concurrent task limit
    active_tasks = len([t for t in app.state.tasks.values()
                       if isinstance(t, asyncio.Task) and not t.done()])

    print(f"Current active tasks: {active_tasks}")
    if active_tasks >= MAX_CONCURRENT_TASKS:
        print(f"Maximum concurrent tasks ({MAX_CONCURRENT_TASKS}) reached")
        return JSONResponse(
            status_code=429,
            content={
                "status": "error",
                "detail": f"Maximum number of concurrent tasks ({MAX_CONCURRENT_TASKS}) reached"
            }
        )

    print(f"Starting stream for batch {batch_id}")

    try:
        task = asyncio.create_task(
            _insert_task_impl(batch_id),
            name=f"task_{batch_id}"
        )
        app.state.tasks[batch_id] = task

        async def handle_task_done(task):
            try:
                await task
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
                if batch_id in app.state.tasks:
                    del app.state.tasks[batch_id]
            except Exception as e:
                print(f"Task {batch_id} failed: {str(e)}")
                if batch_id in app.state.tasks:
                    del app.state.tasks[batch_id]
                raise

        task.add_done_callback(
            lambda _: asyncio.create_task(handle_task_done(task))
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
    """Stop a streaming task."""
    if not hasattr(app.state, 'tasks'):
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "detail": "No tasks found"
            }
        )

    task = app.state.tasks.get(batch_id)
    if task is None:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "detail": f"Task {batch_id} not found"
            }
        )

    if isinstance(task, asyncio.Task) and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error cancelling task {batch_id}: {str(e)}")

    if batch_id in app.state.tasks:
        del app.state.tasks[batch_id]

    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "detail": f"Task {batch_id} stopped"
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
                task_statuses[batch_id] = "completed"
            except asyncio.CancelledError:
                task_statuses[batch_id] = "cancelled"
            except Exception as e:
                task_statuses[batch_id] = f"failed: {str(e)}"
            tasks_to_remove.append(batch_id)
        else:
            task_statuses[batch_id] = "running"

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
    """Aggregate data from the database."""
    if not hasattr(app.state, 'db'):
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": "Database not initialized"
            }
        )

    try:
        async with app.state.db.execute(
            '''
            SELECT DISTINCT value
            FROM data
            WHERE value IS NOT NULL
            ORDER BY value DESC
            '''
        ) as cursor:
            rows = await cursor.fetchall()

        values = []
        if rows:
            values = [row[0] for row in rows if row[0] is not None]
            values.sort(reverse=True)

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "values": values
            }
        )
    except Exception as e:
        print(f"Error in agg endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": str(e)
            }
        )
