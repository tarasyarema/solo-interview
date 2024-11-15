import { useEffect } from "preact/hooks";
import { useSignal } from "@preact/signals";
import { API_URL } from "../routes/api.ts";

type Props = {
  batchId: string;
};

type Data = {
  average: number;
  sum: number;
  count: number;
};

export default function AggregatedData({ batchId }: Props) {
  const data = useSignal<Data>({ average: 0, sum: 0, count: 0 });

  useEffect(() => {
    const interval = setInterval(async () => {
      const res = await fetch(`${API_URL}/agg/${batchId}`);
      data.value = await res.json();
    }, 1000);

    return () => clearInterval(interval);
  }, []);
  return (
    <div class="flex flex-col items-center justify-center">
      <h1 class="text-xl">Aggregated Data</h1>
      <p>Average: {data.value.average.toFixed(2)}</p>
      <p>Sum: {data.value.sum}</p>
      <p>Count: {data.value.count}</p>
    </div>
  );
}
