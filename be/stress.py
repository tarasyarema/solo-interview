import asyncio
import aiohttp
import time

# Configuration
URL = "http://localhost:8000/stream/123"  # Replace with your URL
NUM_REQUESTS = 50  # Number of requests to send

async def send_post_request():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(URL) as response:
                return response.status
        except aiohttp.ClientError as e:
            return f"Error: {e}"

async def main():
    """Send multiple POST requests concurrently."""
    start_time = time.time()

    # Create all tasks at once
    tasks = [asyncio.create_task(send_post_request()) for _ in range(NUM_REQUESTS)]
    
    # Gather all results
    results = await asyncio.gather(*tasks)

    end_time = time.time()
    print(f"Completed {NUM_REQUESTS} requests in {end_time - start_time:.2f} seconds.")
    print("Response statuses:", results)

if __name__ == "__main__":
    asyncio.run(main())

