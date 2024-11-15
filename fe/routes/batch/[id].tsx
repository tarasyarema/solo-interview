import { Handlers, PageProps } from "$fresh/server.ts";
import { API_URL } from "../api.ts";
import Events from "../../islands/Events.tsx";
import BatchDelete from "../../islands/BatchDelete.tsx";
import Error404 from "../_404.tsx";
import AggregatedData from "../../islands/AggregatedData.tsx";

// ! BAD PRACTICE, using GET to trigger a POST request(and also in the back is a GET)
/* export const handler: Handlers = {
  async GET(_req, ctx) {
    // Is this a good idea?
    const req = new Request(`${API_URL}/stream/${ctx.params.id}`, {
      method: "POST",
    });

    await fetch(req);
    return ctx.render();
  },
}; */

export const handler: Handlers = {
  async GET(_req, ctx) {
    const res = await fetch(new Request(`${API_URL}/tasks`));
    const tasks = await res.json();
    return ctx.render(tasks);
  },
};

export default function BatchPage(props: PageProps) {
  if (!props.data.tasks.includes(props.params.id)) {
    return <Error404 />;
  }

  return (
    <div class="max-w-screen-md mx-auto flex flex-col items-center justify-center">
      <div class="flex mb-6">
        <h1 class="text-xl mr-4">
          Batch: <code>{props.params.id}</code>
        </h1>
        <BatchDelete batchId={props.params.id} />
      </div>
      <AggregatedData batchId={props.params.id} />
      <Events batchId={props.params.id} />
    </div>
  );
}
