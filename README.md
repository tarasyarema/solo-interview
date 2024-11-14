# SOLO Interview

> This is the template repo for the SOLO interview process.

## Structure

- [`./fe`](./fe): Frontend project, using `deno` and `fresh`
- [`./be`](./be): Backend project, using `python`, `poetry`, `FastAPI` and `duckdb`

## Instructions

### Before you start

1. Fork this repo (you may want to make the fork private, and share with user `@tarasyarema`)
2. Ensure that you can run both the `fe` and the `be`.

### What you'll find in this repo

#### A `deno` frontend application

> Check the `README.md` in the `fe` folder on how to run.

The frontend is a single page application that will serve as the entrypoint
for the interaction with the backend.

It will serve three main function:

1. (Implemented) Initiate a background processing task
2. See the status of the background processing tasks, and stop them
3. See live aggregated and live data of the background processing tasks

#### A simple `python` backend

The backend is a FastAPI application that will serve as the API for the frontend.

```bash
cd be
poetry install
poetry run fastapi dev main.py
```

By default it wil run on `http://localhost:8000`, you'll see that it's hardcoded in the FE.

It will serve the following (implemented) endpoints:

1. `GET /` that will return the overall data of the background processing tasks
2. `POST /stream/{batch_id}` that will initiate a background processing task for a given `batch_id`
3. `GET /stream/{batch_id}` serves a dummy stream of data for a given `batch_id`

### TODO

> This section is what you need to implement :D

#### Main requirements

- The frontend should have the following screens:
    - `/` with the overall data updated automatically (up to you how fresh you want it to be) and the batch creation form;
    - `/batch/{batch_id}` with the live data and aggregated data for a given `batch_id`, and the stop button (should redirect to `/` after stopping);
- There should be a single task for each `batch_id`;
- The backend should be able to handle multiple `batch_id` at the same time, with a hard limit of 5 (test it);
- The user should be able to move around the various pages smoothly;
- You may do changes to the database, or use additional librarie if needed.
- `GET /agg/{batch_id}` that will return the average, sum and count of the `data.key` values for a given `batch_id` of the last whole minute, i.e. assume `now = 2012-11-11 10:11:10` is the current time, you should return the aggregated values from `2012-11-11 10:10:00` to `2012-11-11 10:11:00` (not included) for the given `batch_id`

- (Optional) Flag any best practices you think are missing in the codebase.
- (Optional) What could be some potential bottlenecks in the current implementation?
