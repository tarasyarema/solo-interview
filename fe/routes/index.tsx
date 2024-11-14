import { Handlers, PageProps } from "$fresh/server.ts";
import BatchCreate from "../islands/BatchCreate.tsx";
import { API_URL } from "./api.ts";

interface Home {
  row_count: number;
  task_count: number;
}

export const handler: Handlers<Home> = {
  async GET(_req, ctx) {
    // Example of fetching data from an API
    const res = await fetch(new Request(`${API_URL}`));
    return ctx.render(await res.json());
  },
};

export default function Home(props: PageProps<Home>) {
  return (
    <div class="max-w-screen-md mx-auto flex flex-col items-center justify-center">
      <h1 class="text-xl font-bold">
        The Batchest
      </h1>
      <p class="my-4">
        The system has <b>{props.data.row_count}</b> rows, and{" "}
        <b>{props.data.task_count}</b> running tasks.
      </p>
      <BatchCreate />
    </div>
  );
}
