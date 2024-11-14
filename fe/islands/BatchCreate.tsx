import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";

export default function BatchCreate() {
  const batchId = useSignal("");
  const disabled = !batchId || batchId.value?.length === 0;

  return (
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
        <Button
          disabled={disabled}
          onClick={() => {
            location.href = `/batch/${batchId.value}`;
          }}
        >
          Create
        </Button>
      )}
    </div>
  );
}
