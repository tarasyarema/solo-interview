import { useEffect } from "preact/hooks";
import { useSignal } from "@preact/signals";
import { API_URL } from "../routes/api.ts";
import { PageProps } from "$fresh/server.ts";
import { Home } from "../routes/index.tsx";

export default function OverallData(props: PageProps<Home>) {
  const data = useSignal(props.data);

  useEffect(() => {
    const interval = setInterval(async () => {
      const res = await fetch(`${API_URL}`);
      data.value = await res.json();
    }, 1000);

    return () => clearInterval(interval);
  }, []);
  return (
    <p class="my-4">
      The system has <b>{data.value.row_count}</b> rows, and{" "}
      <b>{data.value.task_count}</b> running tasks.
    </p>
  );
}
