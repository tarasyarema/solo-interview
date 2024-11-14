import { Handler, PageProps } from "$fresh/server.ts";
import { API_URL } from "../api.ts";
import Events from "../../islands/Events.tsx";

export const handler: Handler = {
  async GET(_req, ctx) {
    // Is this a good idea?
    const req = new Request(
      `${API_URL}/stream/${ctx.params.id}`,
      {
        method: "POST",
      },
    );

    await fetch(req);
    return ctx.render();
  },
};

export default function BatchPage(props: PageProps) {
  return (
    <div class="max-w-screen-md mx-auto flex flex-col items-center justify-center">
      <h1 class="text-xl">
        Batch: <code>{props.params.id}</code>
      </h1>
      <Events batchId={props.params.id} />
    </div>
  );
}
