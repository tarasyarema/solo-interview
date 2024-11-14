import { Handlers, PageProps } from "$fresh/server.ts";
import BatchCreate from "../islands/BatchCreate.tsx";
import { API_URL } from "./api.ts";
import OverallData from "../islands/OverallData.tsx";

export interface Home {
  row_count: number;
  task_count: number;
}

export const handler: Handlers<Home> = {
  async GET(_req, ctx) {
    const res = await fetch(new Request(`${API_URL}`));
    return ctx.render(await res.json());
  },
};

export default function Home(props: PageProps<Home>) {
  return (
    <div class="max-w-screen-md mx-auto flex flex-col items-center justify-center">
      <h1 class="text-xl font-bold">The Batchest</h1>
      <OverallData {...props} />
      <BatchCreate />
    </div>
  );
}
