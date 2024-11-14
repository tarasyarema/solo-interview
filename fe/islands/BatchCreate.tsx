import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";
import { useState } from "preact/hooks";
import { API_URL } from "../routes/api.ts";

export default function BatchCreate() {
  const batchId = useSignal("");
  const disabled = !batchId || batchId.value?.length === 0;
  const [error, setError] = useState<string | null>(null);

  const handleCreate = async () => {
    // * GOOD PRACTICE, using POST to trigger a POST request to create a new batch and not in [id].tsx with GET
    try {
      const res = await fetch(`${API_URL}/stream/${batchId.value}`, {
        method: "POST",
      });
      if (res.ok) {
        location.href = `/batch/${batchId.value}`;
      } else {
        const error = await res.json();
        setError(`Error creating batch: ${error.detail}`);
      }
    } catch (e: any) {
      setError(`Error creating batch: ${e.message}`);
    }
  };

  return (
    <>
      <div class="flex gap-8 py-6">
        <input
          type="text"
          value={batchId}
          onInput={(e) => {
            batchId.value = (e.target as HTMLInputElement).value;
          }}
          placeholder="New batch ID"
          class="px-2 py-1 border-gray-500 border-2 rounded"
        />
        {!disabled && (
          <Button disabled={disabled} onClick={handleCreate}>
            Create
          </Button>
        )}
      </div>
      {error && <p class="text-red">{error}</p>}
    </>
  );
}
